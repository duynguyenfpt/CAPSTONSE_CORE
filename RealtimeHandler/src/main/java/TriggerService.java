import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.streaming.*;

import java.util.Iterator;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

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
//                        .option("failOnDataLoss", "false")
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
                        dataset.foreachPartition(new ForeachPartitionFunction<Row>() {
                            @Override
                            public void call(Iterator<Row> iterator) throws Exception {

                                while (iterator.hasNext()) {
                                    Row row = iterator.next();
                                    String topic = row.getAs("topic");
                                    int latest_offset = row.getAs("latest_offset");
                                    Dataset<Row> changeDF = sparkSession
                                            .read()
                                            .format("kafka")
                                            .option("kafka.bootstrap.servers", "localhost:9092")
                                            .option("subscribe", topic)
                                            .option("startingOffsets", String.format("{\"topic1\":{\"0\":%s}", latest_offset))
                                            .load();
                                }
                            }
                        });
                    }
                })
                .start()
                .awaitTermination();
    }
}
