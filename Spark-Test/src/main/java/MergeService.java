import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.*;
import scala.collection.JavaConverters;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.expressions.Window;

import javax.xml.crypto.Data;

import static org.apache.spark.sql.functions.*;

import java.sql.Connection;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MergeService {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        String topic = args[0];
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("spark-merging")
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
        String bootstrapServer = "localhost:9092";
        String startingOffsets = "latest";
        sparkSession.conf().set("spark.sql.sources.partitionOverwriteMode", "dynamic");
//        String topic = "develop2-127.0.0.1-3306-test-transacton";
//        sparkSession.streams().addListener(new StreamingQueryListener() {
//            @Override
//            public void onQueryStarted(QueryStartedEvent event) {
//                System.out.println("query started" + event.id());
//            }
//
//            @Override
//            public void onQueryProgress(QueryProgressEvent event) {
//                System.out.println("query ended:" + event.progress().id());
//            }
//
//            @Override
//            public void onQueryTerminated(QueryTerminatedEvent event) {
//                System.out.println("Query made progress");
//                System.out.println("Starting offset:" + event.id());
//                System.out.println("Ending offset:" + event.id());
//            }
//        });
        Dataset<Row> readDF =
                sparkSession.readStream()
                        .format("kafka")
                        .option("kafka.bootstrap.servers", bootstrapServer)
                        .option("startingOffsets", startingOffsets)
                        .option("kafka.group.id", "test_123")
//                        .option("failOnDataLoss", "false")
                        .option("subscribe", topic)
                        .load();

        StructType message_schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("database_url", DataTypes.StringType, true),
                DataTypes.createStructField("database_port", DataTypes.StringType, true),
                DataTypes.createStructField("table_name", DataTypes.StringType, true),
                DataTypes.createStructField("schema", DataTypes.StringType, true),
                DataTypes.createStructField("operation", DataTypes.IntegerType, true),
                DataTypes.createStructField("created", DataTypes.LongType, true),
                DataTypes.createStructField("value", DataTypes.StringType, true),
        });
        readDF
                .writeStream()
                .foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
                    @Override
                    public void call(Dataset<Row> rowDataset, Long aLong) throws Exception {
                        rowDataset = rowDataset.withColumn("extract", from_json(col("value").cast("string"), message_schema))
                                .select(col("extract.*"));
                        rowDataset.show(false);
                        List<String> listCastCols = new ArrayList<>();
                        // consume all string first
                        List<StructField> listStructs = new ArrayList<>();
                        //
                        if (rowDataset.count() > 0) {
                            String schema = rowDataset.select("schema").limit(1).collectAsList().get(0).getString(0);
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
                            StructType data_schema = DataTypes.createStructType(listStructs.toArray(new StructField[listStructs.size()]));
                            listCastCols.add("operation");
                            Dataset<Row> transformDF = rowDataset
                                    .withColumn("data", from_json(col("value").cast("string"), data_schema))
                                    .select(col("data.*"), col("operation"));
                            transformDF.show(false);
                            // get current time
                            String currentTime = LocalDateTime.now()
                                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
                            transformDF = transformDF.selectExpr(JavaConverters.asScalaIteratorConverter(listCastCols.iterator()).asScala().toSeq())
                                    .withColumn("modified", lit(currentTime));
                            String partitionBy = "request_date";
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
                            Connection connection = sqlUtils.getConnection(
                                    sqlUtils.getConnectionString("localhost", "3306", "test",
                                            "duynt", "Capstone123@"));
                            //
                            Dataset<Row> source_data = sparkSession.read().parquet("/user/test/test/transacton").alias("ds");
                            Dataset<Row> notAddDS = transformDF.filter("operation <> 1").alias("cdc");
                            source_data.printSchema();
                            notAddDS.printSchema();
                            // merging
                            WindowSpec w1 = Window.partitionBy("request_id");
                            source_data = source_data.join(notAddDS, expr(joinCondition), "left_semi")
                                    .select(source_data.col("*"))
                                    .withColumn("operation", lit(null))
                                    .unionByName(notAddDS)
                                    .withColumn("last____time", max("modified").over(w1))
                                    .filter(expr("modified = last____time and (operation <> 3 or operation is null)"));
                            //
                            if (source_data.count() > 0) {
                                source_data.show();
                                source_data.write()
                                        .mode(SaveMode.Overwrite)
                                        .partitionBy("request_date")
                                        .parquet("/user/test/test/transacton");
                            }
                            //
                            System.out.println("done merge update - delete");
                            if (transformDF.filter("operation = 1").count() > 0) {
                                transformDF.filter("operation = 1").write()
                                        .mode(SaveMode.Append)
                                        .partitionBy("request_date")
                                        .parquet("/user/test/test/transacton");
                            }
                            //
                            System.out.println("done merge insert");
                        }
                    }
                })
                .start()
                .awaitTermination();
        System.out.println("hiehihe");
    }

    public static DataType convertMysqlTypeToSparkType(String type) {
        switch (type) {
            case "int":
            case "smallint":
            case "tinyint":
            case "mediumint":
            case "bit":
            case "year":
                return DataTypes.IntegerType;
            case "float":
                return DataTypes.FloatType;
            case "double":
                return DataTypes.DoubleType;
            case "decimal":
                return new DecimalType(16, 0);
            case "char":
            case "varchar":
            case "text":
            case "mediumtext":
            case "tinytext":
            case "longtext":
            case "enum":
                return DataTypes.StringType;
            case "binary":
            case "varbinary":
            case "blob":
            case "tinyblob":
            case "mediumblob":
            case "longblob":
                return DataTypes.BinaryType;
            case "date":
                return DataTypes.DateType;
            case "datetime":
            case "timestamp":
            case "time":
                return DataTypes.TimestampType;
        }
        return DataTypes.StringType;
    }
}
