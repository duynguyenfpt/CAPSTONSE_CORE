import com.google.gson.Gson;
import models.LogModel;
import models.TableMonitor;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.streaming.*;
import scala.Serializable;
import scala.collection.JavaConverters;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TriggerService implements Serializable {

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
                DataTypes.createStructField("username", DataTypes.StringType, true),
        });

        readDF
                .writeStream()
                .foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
                    @Override
                    public void call(Dataset<Row> dataset, Long aLong) throws Exception {
                        try {
                            dataset = dataset
                                    .withColumn("extract", from_json(col("value").cast("string"), message_schema))
                                    .select(col("extract.*"))
                                    .distinct()
                                    .withColumn("topic"
                                            , concat_ws("-", lit("cdc"), col("server_host"), col("port"),
                                                    col("database"), col("table")));
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
                                try {
                                    // start processing request
                                    // determine number steps
                                    String partitionBy = row.getAs("partition_by");
                                    //
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
                                    //
                                    //
                                    LogModel log = new LogModel(row.getAs("job_id"), row.getAs("str_id"),
                                            row.getAs("server_host"), row.getAs("port"),
                                            row.getAs("database"), row.getAs("table"), 1, "sync_all",
                                            12, null, null);
                                    // start reading data
                                    Dataset<Row> dataset1 = null;
                                    try {
                                        sendLogs(1, "processing", "reading from data changed", log, row, connection);
                                        dataset1 = sparkSession
                                                .read()
                                                .format("kafka")
                                                .option("kafka.bootstrap.servers", "localhost:9092")
                                                .option("subscribe", topic_req)
                                                .option("startingOffsets", so)
                                                .load()
                                                .withColumn("extract", from_json(col("value").cast("string"), data_schema))
                                                .select(col("extract.*"));
                                        sendLogs(1, "success", "done reading from data changed", log, row, connection);
                                    } catch (Exception exception) {
                                        exception.printStackTrace();
                                        String messageError = "Can't not read from kafka source !";
                                        sendLogs(1, "fail", messageError, log, row, connection);
                                        throw new Exception(messageError);
                                    }
                                    // set is ready = false
                                    try {
                                        sendLogs(2, "processing", "update readiness", log, row, connection);
                                        sqlUtils.updateReady(row.getAs("server_host"), row.getAs("port")
                                                , row.getAs("database"), row.getAs("table"), connection, 0);
                                        sendLogs(2, "success", "done update readiness", log, row, connection);
                                    } catch (Exception exception) {
                                        exception.printStackTrace();
                                        String messageError = "Can't not update readiness in config database";
                                        sendLogs(2, "success", messageError, log, row, connection);
                                        throw new Exception(messageError);
                                    }
                                    //
                                    if (dataset1.count() > 0) {
                                        long numberRecords = dataset1.count();
                                        dataset1.show(false);
                                        List<String> listCastCols = new ArrayList<>();
                                        // consume all string first
                                        List<StructField> listStructs = new ArrayList<>();
                                        Dataset<Row> transformDF = null;
                                        // reading table metadata
                                        try {
                                            sendLogs(3, "processing", "reading table metadata", log, row, connection);
                                            String schema = dataset1.select("schema").limit(1).collectAsList().get(0).getString(0);
                                            Pattern pattern = Pattern.compile("\"([a-zA-Z1-9_:]+)");
                                            Matcher matcher = pattern.matcher(schema);
                                            HashMap<String, String> fieldTypeMap = new HashMap<>();
                                            while (matcher.find()) {
                                                String[] detail = matcher.group(1).split(":");
                                                if (detail[1].equals("text") || detail[1].contains("VARCHAR") || detail[1].contains("varchar") || detail[1].equals("NUMBER")) {
                                                    detail[1] = "string";
                                                }
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
                                            // converting type
                                            transformDF = dataset1
                                                    .withColumnRenamed("id", "ssk_id")
                                                    .withColumn("data", from_json(col("value").cast("string"), dataSchema))
                                                    .select(col("data.*"), col("operation"), col("ssk_id"));
                                            //
//                                        transformDF.show();
//                                        System.out.println("show max");
//                                        transformDF.agg(max(col("ssk_id")).alias("max_id")).show();
                                            sendLogs(3, "success", "done reading table metadata", log, row, connection);
                                        } catch (Exception exception) {
                                            exception.printStackTrace();
                                            String messageError = "Can't not convert data type";
                                            sendLogs(3, "fail", messageError, log, row, connection);
                                            throw new Exception(messageError);
                                        }
                                        //
                                        int maxID = 0;
                                        try {
                                            sendLogs(4, "processing", "get max ID", log, row, connection);
                                            maxID = transformDF.agg(max(col("ssk_id")).alias("max_id")).collectAsList().get(0).getInt(0);
                                            sendLogs(4, "success", "done get max ID", log, row, connection);
                                        } catch (Exception exception) {
                                            exception.printStackTrace();
                                            String messageError = "Can't not get ID";
                                            sendLogs(4, "fail", messageError, log, row, connection);
                                            throw new Exception(messageError);
                                        }
                                        WindowSpec windows = Window.partitionBy(row.getAs("identity_key"), "operation");
                                        // remove duplication operation - only get the last one
                                        try {
                                            sendLogs(5, "processing", "remove duplication operations", log, row, connection);
                                            transformDF = transformDF
                                                    .withColumn("latest_operation", max("ssk_id").over(windows))
                                                    .filter(expr("ssk_id = latest_operation"))
                                                    .drop("ssk_id", "latest_operation");
                                            transformDF.show(false);
                                            transformDF.printSchema();
                                            // get current time
                                            String currentTime = LocalDateTime.now()
                                                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
                                            transformDF = transformDF.selectExpr(JavaConverters.asScalaIteratorConverter(listCastCols.iterator()).asScala().toSeq())
                                                    .withColumn("modified", lit(currentTime));
                                            sendLogs(5, "success", "done removing duplication operations", log, row, connection);
                                        } catch (Exception exception) {
                                            exception.printStackTrace();
                                            String messageError = "Can't not remove duplicates";
                                            sendLogs(5, "fail", messageError, log, row, connection);
                                            throw new SparkException(messageError);
                                        }

                                        // reading data source
                                        Dataset<Row> source_data = null;
                                        try {
                                            sendLogs(6, "processing", "reading data source", log, row, connection);
                                            String databaseType = row.getAs("database_type");
                                            String host = row.getAs("server_host");
                                            String port = row.getAs("port");
                                            String database = row.getAs("database");
                                            String table = row.getAs("table");
                                            String username = row.getAs("username");
                                            if (!databaseType.equals("oracle")) {
                                                source_data = sparkSession.read().parquet(String.format("/user/%s/%s/%s/%s/%s/", databaseType, host, port, database, table)).alias("ds");
                                            } else {
                                                source_data = sparkSession.read().parquet(String.format("/user/%s/%s/%s/%s/%s/", databaseType, host, port, username, table)).alias("ds");
                                            }
                                            sendLogs(6, "success", "done reading data source", log, row, connection);
                                        } catch (Exception exception) {
                                            exception.printStackTrace();
                                            String messageError = "Can't not read data source";
                                            sendLogs(6, "fail", messageError, log, row, connection);
                                            throw new Exception(messageError);
                                        }
                                        if (Objects.isNull(partitionBy) || partitionBy.equals("")) {
                                            // merging
                                            try {
                                                sendLogs(7, "processing", "merging", log, row, connection);
                                                WindowSpec w1 = Window.partitionBy(row.<String>getAs("identity_key"));
                                                transformDF.show(false);
                                                source_data = source_data.withColumn("is_new", lit(0))
                                                        .unionByName(transformDF.drop("operation").withColumn("is_new", lit(1)))
                                                        .withColumn("latest_id", max("is_new").over(w1));
                                                System.out.println("after union");
                                                source_data.filter("id = 1").show();
                                                source_data = source_data
                                                        .filter("latest_id = is_new")
                                                        .drop("is_new", "latest_id");

                                                sendLogs(7, "success", "done merging", log, row, connection);
                                            } catch (Exception exception) {
                                                exception.printStackTrace();
                                                String messageError = "Merge failed !";
                                                sendLogs(7, "fail", messageError, log, row, connection);
                                                throw new Exception(messageError);
                                            }
                                            // write data to temp location
                                            Dataset<Row> tmp = null;
                                            try {
                                                sendLogs(8, "processing", "write data to temp location", log, row, connection);
                                                source_data.write()
                                                        .mode("overwrite")
                                                        .parquet(String.format("/user/tmp/%s/%s", row.getAs("database"), row.getAs("table")));
                                                tmp = sparkSession.read().parquet(String.format("/user/tmp/%s/%s", row.getAs("database"), row.getAs("table")));
                                                sendLogs(8, "success", "done writing data to temp location", log, row, connection);
                                            } catch (Exception exception) {
                                                exception.printStackTrace();
                                                String messageError = "Write to temp location failed !";
                                                sendLogs(8, "fail", messageError, log, row, connection);
                                                throw new Exception(messageError);
                                            }
                                            // move from location to main location
                                            try {
                                                String databaseType = row.getAs("database_type");
                                                String host = row.getAs("server_host");
                                                String port = row.getAs("port");
                                                String database = row.getAs("database");
                                                String table = row.getAs("table");
                                                String username = row.getAs("username");
                                                if (!databaseType.equals("oracle")) {
                                                    tmp.write().mode("overwrite").parquet(String.format("/user/%s/%s/%s/%s/%s/", databaseType, host, port, database, table));
                                                } else {
                                                    tmp.write().mode("overwrite").parquet(String.format("/user/%s/%s/%s/%s/%s/", databaseType, host, port, username, table));
                                                }
                                                sendLogs(9, "processing", "move from location to main location", log, row, connection);
//                                                tmp.write()
//                                                        .mode("overwrite")
//                                                        .parquet(String.format("/user/test/%s/%s", row.getAs("database"), row.getAs("table")));
                                                sendLogs(9, "success", "done moving from location to main location", log, row, connection);
                                            } catch (Exception exception) {
                                                exception.printStackTrace();
                                                String messageError = "Fail moving from location to main location";
                                                sendLogs(9, "success", messageError, log, row, connection);
                                                throw new Exception(messageError);
                                            }
                                        } else {
                                            // creating join condition
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
                                            Dataset<Row> notAddDS = null;
                                            try {

                                                sendLogs(7, "processing", "get update/delete data", log, row, connection);
                                                notAddDS = transformDF.filter("operation <> 1").alias("cdc");
                                                sendLogs(7, "success", "done getting update/delete data", log, row, connection);
                                            } catch (Exception exception) {
                                                exception.printStackTrace();
                                                String messageError = "Get update/delete data failed !";
                                                sendLogs(7, "success", messageError, log, row, connection);
                                                throw new Exception(messageError);
                                            }
//                                        source_data.printSchema();
//                                        notAddDS.printSchema();
                                            // union new data arrived
                                            WindowSpec w1 = Window.partitionBy(row.<String>getAs("identity_key"));
                                            try {
                                                source_data = source_data.join(notAddDS, expr(joinCondition), "left_semi")
                                                        .select(source_data.col("*"))
                                                        .withColumn("operation", lit(null))
                                                        .unionByName(notAddDS)
                                                        .withColumn("last____time", max("modified").over(w1))
                                                        .filter(expr("modified = last____time and (operation <> 3 or operation is null)"));
                                                //
                                                sendLogs(8, "processing", "merge update/delete data", log, row, connection);
                                                String databaseType = row.getAs("database_type");
                                                String host = row.getAs("server_host");
                                                String port = row.getAs("port");
                                                String database = row.getAs("database");
                                                String table = row.getAs("table");
                                                String username = row.getAs("username");
                                                String path = "";
                                                if (!databaseType.equals("oracle")) {
                                                    path = String.format("/user/%s/%s/%s/%s/%s/", databaseType, host, port, database, table);
                                                } else {
                                                    path = String.format("/user/%s/%s/%s/%s/%s/", databaseType, host, port, username, table);
                                                }
                                                if (source_data.count() > 0) {
                                                    // rewrite data updated and deleted
                                                    source_data.show();
                                                    source_data.write()
                                                            .mode(SaveMode.Overwrite)
                                                            .partitionBy(partitionBy)
                                                            .parquet(path);
                                                }
                                                //
                                                sendLogs(8, "success", "done merging update/delete data", log, row, connection);
                                                sendLogs(9, "processing", "merge insert data", log, row, connection);
                                                if (transformDF.filter("operation = 1").count() > 0) {
                                                    // append new data inserted
                                                    transformDF.filter("operation = 1").write()
                                                            .mode(SaveMode.Append)
                                                            .partitionBy(partitionBy)
                                                            .parquet(path);
                                                }
                                                sendLogs(9, "success", "done merging insert data", log, row, connection);
                                            } catch (Exception exception) {
                                                exception.printStackTrace();
                                                String messageError = "fail merging: " + exception.getMessage();
                                                sendLogs(9, "fail", messageError, log, row, connection);
                                                throw new Exception(messageError);
                                            }
                                        }
                                        int latest_id;
                                        try {
                                            sendLogs(10, "processing", "get latest id", log, row, connection);
                                            latest_id = sqlUtils.getLatestId(row.getAs("server_host"), row.getAs("port"),
                                                    row.getAs("database"), row.getAs("table"), connection);
                                            sendLogs(10, "success", "done getting latest id", log, row, connection);
                                        } catch (Exception exception) {
                                            exception.printStackTrace();
                                            String messageError = "fail getting latest id";
                                            sendLogs(10, "fail", messageError, log, row, connection);
                                            throw new Exception(messageError);
                                        }
                                        // update readiness
                                        try {
                                            sendLogs(11, "processing", "update readiness", log, row, connection);
                                            if (maxID < latest_id) {
                                                sqlUtils.updateReady(row.getAs("server_host"), row.getAs("port")
                                                        , row.getAs("database"), row.getAs("table"), connection, 1);
                                                System.out.println("updated readiness");
                                            }
                                            sendLogs(11, "success", "done updating readiness", log, row, connection);
                                        } catch (Exception exception) {
                                            exception.printStackTrace();
                                            String messageError = "fail updating readiness";
                                            sendLogs(11, "fail", messageError, log, row, connection);
                                            throw new Exception(messageError);
                                        }
                                        // update offsets
                                        try {
                                            sendLogs(12, "processing", "update offsets kafka", log, row, connection);
                                            sqlUtils.updateOffset(connection, row.getAs("server_host"), row.getAs("port")
                                                    , row.getAs("database"), row.getAs("table"), (int) (latest_offset + numberRecords));
                                            sendLogs(12, "success", "done updating offsets kafka", log, row, connection);
                                        } catch (Exception exception) {
                                            exception.printStackTrace();
                                            String messageError = "fail updating offsets kafka";
                                            sendLogs(12, "success", messageError, log, row, connection);
                                            throw new Exception(messageError);
                                        }
                                    }
                                } catch (Exception exception) {
                                    // retrying
                                }
                            }
                        } catch (Exception exception) {
                            System.out.println(exception.getMessage());
                            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                            Date date = new Date();
                            System.out.println("JOBS FAIL AT: " + dateFormat.format(date));
                        }
                    }
                })
                .start()
                .awaitTermination();
    }

    public static void sendLogs(int step, String status, String message, LogModel log, Row row, Connection connection) throws SQLException {
        log.setStep(step);
        log.setStatus(status);
        log.setMessage(message);
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        Date date = new Date();
        log.setCreate_time(dateFormat.format(date));
        if (status.equals("success")) {
            log.setStatusOrder(3);
            sqlUtils.logProducer("localhost:9092", "jobs_log", log);
            sqlUtils.logProducer("localhost:9092", "jobs_log", log);
        } else if (status.equals("fail")) {
            log.setStatusOrder(2);
            sqlUtils.logProducer("localhost:9092", "jobs_log", log);
            TableMonitor tm = new TableMonitor();
            // retrying
            try {
                tm.setServer_host(row.getAs("server_host"));
                tm.setPort(row.getAs("port"));
                tm.setDatabase_type(row.getAs("database_type"));
                tm.setIdentity_key(row.getAs("identity_key"));
                tm.setPartition_by(row.getAs("partition_by"));
                tm.setTable(row.getAs("table"));
                tm.setStr_id(row.getAs("str_id"));
                tm.setMax_retries(row.getAs("max_retries"));
                tm.setNumber_retries(row.getAs("number_retries"));
                tm.setNumber_retries(tm.getNumber_retries() + 1);
                tm.setLatest_offset(row.getAs("latest_offset"));
                tm.setDatabase(row.getAs("database"));
                tm.setJob_id(row.getAs("job_id"));
            } catch (Exception exception) {
                exception.printStackTrace();
            }
            // retries
            System.out.println(tm);
            //
            //
            log.setStep(0);
            log.setMessage("waiting for retrying number: " + (tm.getNumber_retries() + 1));
            log.setStatus("retrying");
            sqlUtils.logProducer("localhost:9092", "jobs_log", log);
            // insert retry count
            System.out.println("number_retries" + tm.getNumber_retries());
            System.out.println("max_retries" + tm.getMax_retries());
            if (tm.getNumber_retries() >= tm.getMax_retries()) {
                String query = "update webservice_test.jobs\n" +
                        "set is_active = b'0' \n" +
                        "where id = ?;";
                PreparedStatement prpStmt = connection.prepareStatement(query);
                prpStmt.setInt(1, tm.getJob_id());
                prpStmt.executeUpdate();
            } else {
                String query = "update webservice_test.jobs\n" +
                        "set number_retries = number_retries + 1\n" +
                        "where id = ?;";
                PreparedStatement prpStmt = connection.prepareStatement(query);
                prpStmt.setInt(1, tm.getJob_id());
                prpStmt.executeUpdate();
                sqlUtils.applicationProducer("localhost:9092", "cdc-table-change-test", tm);
            }
        } else if (status.equals("processing")) {
            log.setStatusOrder(1);
            sqlUtils.logProducer("localhost:9092", "jobs_log", log);
        }

    }
}
