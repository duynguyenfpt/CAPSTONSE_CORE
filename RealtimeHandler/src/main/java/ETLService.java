import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

public class ETLService {
    public static void main(String[] args) throws SQLException, TimeoutException, StreamingQueryException {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Tracking Events")
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
        String bootstrapServer = "localhost:9092";
        String startingOffsets = "latest";
        String topic = "etl-query";
        sparkSession.conf().set("spark.sql.sources.partitionOverwriteMode", "dynamic");
        //
        //
        Connection configConnection = sqlUtils.getConnection(
                sqlUtils.getConnectionString("localhost", "3306", "cdc",
                        "duynt", "Capstone123@"));
        // get alias and path on hdfs
        ResultSet rs = getAlias(configConnection);
        HashMap<String, String> aliasHashMap = new HashMap<>();
        while (rs.next()) {
            String path = String.format("/user/%s/%s/%s/%s/%s/", rs.getString("database_type"),
                    rs.getString("server_host"), rs.getString("port"),
                    rs.getString("database_name"), rs.getString("table_name"));
            // set up alias
            try {
                String queryAlias = ":" + rs.getString("alias") + "." + rs.getString("table_name") + ":";
                String alias = "table" + rs.getInt("table_id");
                // create temp view in spark
                System.out.println(path);
                sparkSession.read().parquet(path).createOrReplaceTempView(alias);
                System.out.println(String.format("%s alias as: %s - path: ", queryAlias, alias, path));
                aliasHashMap.put(queryAlias, alias);
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        }
        System.out.println(aliasHashMap.get(":sales.public.employees:"));
        //
        Dataset<Row> queryDF =
                sparkSession.readStream()
                        .format("kafka")
                        .option("kafka.bootstrap.servers", bootstrapServer)
                        .option("startingOffsets", startingOffsets)
                        .option("kafka.group.id", "test_123")
                        .option("failOnDataLoss", "false")
                        .option("subscribe", topic)
                        .load();
        //
        StructType querySchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("jobId", DataTypes.IntegerType, false),
                DataTypes.createStructField("requestId", DataTypes.IntegerType, true),
                DataTypes.createStructField("etlID", DataTypes.IntegerType, true),
                DataTypes.createStructField("query", DataTypes.StringType, true)
        });
        //
        queryDF
                .writeStream()
                .foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
                    @Override
                    public void call(Dataset<Row> dataset, Long aLong) throws Exception {
                        try {
                            dataset = dataset
                                    .withColumn("extract", from_json(col("value").cast("string"), querySchema))
                                    .select(col("extract.*"));
                            dataset.show(false);
                            List<Row> list_request = dataset.collectAsList();
                            for (Row row : list_request) {
                                try {
                                    int jobId = row.getAs("jobId");
                                    int requestId = row.getAs("requestId");
                                    int etlID = row.getAs("etlID");
                                    String query = row.getAs("query");
                                    // query parser
                                    query = queryTableConverter(query, aliasHashMap);
                                    String path = String.format("/user/etl_query/id_%d_%d/", requestId, jobId);
                                    // execute query
                                    Dataset<Row> result = sparkSession.sql(query);
                                    long total = result.count();
                                    result
                                            .coalesce(1)
                                            .write()
                                            .mode("overwrite")
                                            .option("header", true)
                                            .csv(path);
                                    //
                                    FileSystem fs = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());
                                    RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(path), false);
                                    String fileName = "";
                                    while (listFiles.hasNext()) {
                                        fileName = listFiles.next().getPath().getName();
                                        if (!fileName.equalsIgnoreCase("_SUCCESS")) {
                                            break;
                                        }
                                    }

                                    // set status success
                                    System.out.println("update status");
                                    String updateQuery = "update webservice_test.jobs set status = 'success' where id = ?";
                                    PreparedStatement prpStmt = configConnection.prepareStatement(updateQuery);
                                    prpStmt.setInt(1, jobId);
                                    prpStmt.executeUpdate();
                                    //
                                    System.out.println("update path");
                                    String updateQuery2 = "update webservice_test.etl_request set result_path = ?,total_rows = ? where id = ?";
                                    PreparedStatement prpStmt2 = configConnection.prepareStatement(updateQuery2);
                                    prpStmt2.setString(1, path + fileName);
                                    prpStmt2.setLong(2, total);
                                    prpStmt2.setInt(3, etlID);
                                    prpStmt2.executeUpdate();
                                    //
                                    System.out.println("done hehe");
                                } catch (Exception exception) {
                                    exception.printStackTrace();
                                }
                            }
                        } catch (Exception exception) {
                            exception.printStackTrace();
                        }
                    }
                })
                .start()
                .awaitTermination();
    }

    public static ResultSet getAlias(Connection connection) throws SQLException {
        String query = "" +
                "SELECT di.database_type,si.server_host,di.port,di.database_name,tbls.table_name, di.alias,tbls.id as table_id FROM webservice_test.database_infos di\n" +
                "INNER JOIN webservice_test.`tables` tbls\n" +
                "INNER JOIN webservice_test.server_infos si\n" +
                "ON tbls.database_info_id = di.id and di.server_info_id = si.id\n" +
                "WHERE di.deleted = 0\n" +
                "ORDER BY server_host, port;";
        //
        Statement stmt = connection.createStatement();
        //
        return stmt.executeQuery(query);
    }

    public static String queryTableConverter(String tableQuery, HashMap<String, String> aliasHM) throws SQLException {
        ArrayList<String> result = new ArrayList<>();
        Pattern pattern = Pattern.compile(":[a-zA-Z1-9.]+:");
        Matcher matcher = pattern.matcher(tableQuery);
        while (matcher.find()) {
            String queryAlias = matcher.group(0);
            System.out.println(queryAlias);
            tableQuery = tableQuery.replaceAll(queryAlias, aliasHM.get(queryAlias));
        }
        return tableQuery;
    }

    ;
}
