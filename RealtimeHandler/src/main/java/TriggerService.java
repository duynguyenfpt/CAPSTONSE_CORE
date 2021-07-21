import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.streaming.*;
import scala.collection.JavaConverters;

import java.sql.Connection;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TriggerService {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Tracking Events")
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
        String bootstrapServer = "localhost:9092";
        String startingOffsets = "latest";
        String topic = "cdc-table-change-test";
        sparkSession.conf().set("spark.sql.sources.partitionOverwriteMode", "dynamic");
        Dataset<Row> readDF =
                sparkSession.readStream()
                        .format("kafka")
                        .option("kafka.bootstrap.servers", bootstrapServer)
                        .option("startingOffsets", startingOffsets)
                        .option("kafka.group.id", "test_123")
                        .option("failOnDataLoss", "false")
                        .option("subscribe", topic)
                        .load();
        StructType message_schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("server_host", DataTypes.StringType, false),
                DataTypes.createStructField("port", DataTypes.StringType, true),
                DataTypes.createStructField("table", DataTypes.StringType, true),
                DataTypes.createStructField("database_type", DataTypes.StringType, true),
                DataTypes.createStructField("identity_key", DataTypes.StringType, true),
                DataTypes.createStructField("partition_by", DataTypes.StringType, true),
                DataTypes.createStructField("str_id", DataTypes.IntegerType, true),
                DataTypes.createStructField("job_id", DataTypes.IntegerType, true),
                DataTypes.createStructField("max_retries", DataTypes.IntegerType, true),
                DataTypes.createStructField("number_retries", DataTypes.IntegerType, true),
                DataTypes.createStructField("latest_offset", DataTypes.IntegerType, true),
                DataTypes.createStructField("database", DataTypes.StringType, true),
        });

        readDF
                .writeStream()
                .foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
                    @Override
                    public void call(Dataset<Row> dataset, Long aLong) throws Exception {
//                        String topicName = "cdc-" + host + "-" + port + "-" + db + "-" + current_table;
                        dataset = dataset
                                .withColumn("extract", from_json(col("value").cast("string"), message_schema))
                                .select(col("extract.*"))
                                .distinct()
                                .withColumn("topic"
                                        , concat_ws("-", lit("cdc"), col("server_host"), col("port"),
                                                col("database"), col("table")));
                        //

                        dataset.printSchema();
                        //
                        System.out.println("Execute Distinct: " + dataset.count());
                        //
                        dataset.select("topic").show(false);
                        dataset.show();
                        List<Row> list_request = dataset.collectAsList();
                        //
                        Connection connection = sqlUtils.getConnection(
                                sqlUtils.getConnectionString("localhost", "3306", "cdc",
                                        "duynt", "Capstone123@"));
                        //
                        for (Row row : list_request) {
                            int latest_offset = row.getAs("latest_offset");
                            String topic_req = row.getAs("topic");
                            String so = String.format("{\"%s\":{\"0\":%d}}", topic_req, latest_offset);
                            System.out.println(so);
                            StructType data_schema = DataTypes.createStructType(new StructField[]{
                                    DataTypes.createStructField("id", DataTypes.IntegerType, false),
                                    DataTypes.createStructField("database_url", DataTypes.StringType, true),
                                    DataTypes.createStructField("database_port", DataTypes.StringType, true),
                                    DataTypes.createStructField("table_name", DataTypes.StringType, true),
                                    DataTypes.createStructField("schema", DataTypes.StringType, true),
                                    DataTypes.createStructField("operation", DataTypes.IntegerType, true),
                                    DataTypes.createStructField("created", DataTypes.LongType, true),
                                    DataTypes.createStructField("value", DataTypes.StringType, true),
                            });
                            Dataset<Row> dataset1 = sparkSession
                                    .read()
                                    .format("kafka")
                                    .option("kafka.bootstrap.servers", "localhost:9092")
                                    .option("subscribe", topic_req)
                                    .option("startingOffsets", so)
                                    .load()
                                    .withColumn("extract", from_json(col("value").cast("string"), data_schema))
                                    .select(col("extract.*"));
                            // set is ready = false
                            sqlUtils.updateReady(row.getAs("server_host"), row.getAs("port")
                                    , row.getAs("database"), row.getAs("table"), connection, 0);
                            //
                            long numberRecords = dataset1.count();
                            dataset1.show(false);
                            List<String> listCastCols = new ArrayList<>();
                            // consume all string first
                            List<StructField> listStructs = new ArrayList<>();
                            if (dataset1.count() > 0) {
                                String schema = dataset1.select("schema").limit(1).collectAsList().get(0).getString(0);
                                Pattern pattern = Pattern.compile("\"([a-z_:]+)");
                                Matcher matcher = pattern.matcher(schema);
                                HashMap<String, String> fieldTypeMap = new HashMap<>();
                                while (matcher.find()) {
                                    String[] detail = matcher.group(1).split(":");
                                    if (!detail[1].contains("(")) {
                                        fieldTypeMap.put(detail[0], detail[1]);
                                        listCastCols.add(String.format("cast (%s as %s) as %s", detail[0], detail[1], detail[0]));
                                    } else {
                                        fieldTypeMap.put(detail[0], detail[1].substring(0, detail[1].indexOf("(")));
                                        listCastCols.add(String.format("cast (%s as %s) as %s", detail[0],
                                                detail[1].substring(0, detail[1].indexOf("(")), detail[0]));
                                    }
                                    listStructs.add(DataTypes.createStructField(detail[0], DataTypes.StringType, false));
                                }
                                StructType dataSchema = DataTypes.createStructType(listStructs.toArray(new StructField[listStructs.size()]));
                                listCastCols.add("operation");
                                Dataset<Row> transformDF = dataset1
                                        .withColumn("data", from_json(col("value").cast("string"), dataSchema))
                                        .select(col("data.*"), col("operation"), col("id"));
                                //
                                int maxID = transformDF.agg(max(col("id")).alias("max_id")).collectAsList().get(0).getInt(0);
                                System.out.println("max_id is : " + maxID);
                                //
                                transformDF = transformDF.drop("id");
                                transformDF.show(false);
                                // get current time
                                String currentTime = LocalDateTime.now()
                                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
                                transformDF = transformDF.selectExpr(JavaConverters.asScalaIteratorConverter(listCastCols.iterator()).asScala().toSeq())
                                        .withColumn("modified", lit(currentTime));
                                String partitionBy = row.getAs("partition_by");
                                //
                                String joinCondition = "";
                                int index = 0;
                                for (String col : partitionBy.split(",")) {
                                    joinCondition += String.format("cdc.%s = ds.%s ", col, col);
                                    if (index < partitionBy.split(",").length - 1) {
                                        joinCondition += "and ";
                                    }
                                }
                                System.out.println("join condition: " + joinCondition);
                                //
                                Dataset<Row> source_data = sparkSession.read().parquet(String.format("/user/test/%s/%s", row.getAs("database"), row.getAs("table"))).alias("ds");
                                Dataset<Row> notAddDS = transformDF.filter("operation <> 1").alias("cdc");
                                source_data.printSchema();
                                notAddDS.printSchema();
                                // merging
                                WindowSpec w1 = Window.partitionBy(row.<String>getAs("identity_key"));
                                source_data = source_data.join(notAddDS, expr(joinCondition), "left_semi")
                                        .select(source_data.col("*"))
                                        .withColumn("operation", lit(null))
                                        .unionByName(notAddDS)
                                        .withColumn("last____time", max("modified").over(w1))
                                        .filter(expr("modified = last____time and (operation <> 3 or operation is null)"));
                                //
//                                if (source_data.count() > 0) {
//                                    source_data.show();
//                                    source_data.write()
//                                            .mode(SaveMode.Overwrite)
//                                            .partitionBy("request_date")
//                                            .parquet(String.format("/user/test/%s/%s", row.getAs("database"), row.getAs("table")));
//                                }
//                                //
//                                System.out.println("done merge update - delete");
//                                if (transformDF.filter("operation = 1").count() > 0) {
//                                    transformDF.filter("operation = 1").write()
//                                            .mode(SaveMode.Append)
//                                            .partitionBy("request_date")
//                                            .parquet(String.format("/user/test/%s/%s", row.getAs("database"), row.getAs("table")));
//                                }
                                //
                                System.out.println("done merge insert");
                            }

                        }
                        System.out.println("doneee");
                    }
                })
                .start()
                .awaitTermination();
    }
}
