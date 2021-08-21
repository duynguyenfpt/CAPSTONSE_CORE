import com.fasterxml.jackson.databind.ObjectMapper;
import models.LogModel;
import models.MappingModel;
import models.MergeRequest;
import models.TableMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.streaming.*;
import scala.Serializable;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TriggerService implements Serializable {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("CDC Updates")
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
        String bootstrapServer = "localhost:9092";
        String startingOffsets = "latest";
        String topic = "cdc-table-change-test,merge-request";
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

        StructType merge_schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("host", DataTypes.StringType, false),
                DataTypes.createStructField("port", DataTypes.StringType, true),
                DataTypes.createStructField("table", DataTypes.StringType, true),
                DataTypes.createStructField("databaseType", DataTypes.StringType, true),
                DataTypes.createStructField("database", DataTypes.StringType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("mergeTable", DataTypes.StringType, true),
        });

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
                    public void call(Dataset<Row> requestedDataset, Long aLong) throws Exception {
                        try {
                            Dataset<Row> dataset = requestedDataset
                                    .filter("topic = 'cdc-table-change-test'")
                                    .withColumn("extract", from_json(col("value").cast("string"), message_schema))
                                    .select(col("extract.*"))
                                    .distinct()
                                    .withColumn("topic_cdc"
                                            , concat_ws("-", lit("cdc"), col("server_host"), col("port"),
                                                    col("database"), col("table")));
                            //
                            System.out.println("Execute Distinct: " + dataset.count());
                            //
                            dataset.select("topic_cdc").show(false);
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
                                    String topic_req = row.getAs("topic_cdc");
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
                                    Dataset<Row> cdcMergeDS = null;
                                    //
                                    String databaseType = row.getAs("database_type");
                                    String host = row.getAs("server_host");
                                    String port = row.getAs("port");
                                    String database = row.getAs("database");
                                    String table = row.getAs("table");
                                    String username = row.getAs("username");
                                    String path = "";
                                    String schema = "";
                                    String identity_key = row.getAs("identity_key");
                                    if (identity_key == null || identity_key.equals("")) {
                                        identity_key = getDefaultKeys(connection, host, port, database, table);
                                    }
                                    System.out.println("identity_key: " + identity_key);
                                    if (!databaseType.equals("oracle")) {
                                        path = String.format("/user/%s/%s/%s/%s/%s/", databaseType, host, port, database, table);
                                    } else {
                                        path = String.format("/user/%s/%s/%s/%s/%s/", databaseType, host, port, username, table);
                                    }
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
                                        dataset1.show();
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
                                        List<String> listCDCCols = new ArrayList<>();
                                        Dataset<Row> transformDF = null;
                                        // reading table metadata
                                        try {
                                            sendLogs(3, "processing", "reading table metadata", log, row, connection);
                                            //
                                            Set<String> combineSchema = new HashSet<>();
                                            List<Row> listSchema = dataset1.select("schema").collectAsList();
                                            for (Row rowSchema : listSchema) {
                                                Pattern pattern = Pattern.compile("\"([a-zA-Z1-9_:]+)");
                                                Matcher matcher = pattern.matcher(rowSchema.getString(0));
                                                HashMap<String, String> fieldTypeMap = new HashMap<>();
                                                while (matcher.find()) {
                                                    combineSchema.add(matcher.group(1));
                                                }
                                            }
                                            for (String col : combineSchema) {
                                                schema = schema + "\"" + col + ",";
                                            }
                                            System.out.println("schema is: " + schema);
                                            //
                                            Pattern pattern = Pattern.compile("\"([a-zA-Z1-9_:]+)");
                                            Matcher matcher = pattern.matcher(schema);
                                            HashMap<String, String> fieldTypeMap = new HashMap<>();
                                            while (matcher.find()) {
                                                String[] detail = matcher.group(1).split(":");
                                                if (detail[1].equalsIgnoreCase("text") || detail[1].equalsIgnoreCase("VARCHAR") ||
                                                        detail[1].contains("varchar") || detail[1].equals("NUMBER") || detail[1].equalsIgnoreCase("character")) {
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
                                                listCDCCols.add(detail[0].toUpperCase());
                                            }
                                            StructType dataSchema = DataTypes.createStructType(listStructs.toArray(new StructField[listStructs.size()]));
                                            listCDCCols.add("OPERATION");
                                            listCastCols.add("OPERATION");
                                            // converting type
                                            // compress the data
                                            transformDF = dataset1
                                                    .withColumnRenamed("id", "ssk_id")
                                                    .withColumn("data", from_json(col("value").cast("string"), dataSchema))
                                                    .select(col("data.*"), col("OPERATION"), col("ssk_id"));
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
                                        WindowSpec windows = null;
                                        try {
                                            windows = Window.partitionBy(identity_key, "operation");
                                        } catch (Exception exception) {
                                            exception.printStackTrace();
                                        }
                                        // remove duplication operation - only get the last one
                                        try {
                                            sendLogs(5, "processing", "remove duplication operations", log, row, connection);
                                            transformDF = transformDF
                                                    .withColumn("latest_operation", max("ssk_id").over(windows))
                                                    .filter(expr("ssk_id = latest_operation"))
                                                    .drop("ssk_id", "latest_operation");
                                            // get current time
                                            String currentTime = LocalDateTime.now()
                                                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
                                            transformDF = transformDF.selectExpr(JavaConverters.asScalaIteratorConverter(listCastCols.iterator()).asScala().toSeq())
                                                    .withColumn("modified", lit(currentTime));
                                            transformDF.show(false);
                                            transformDF.printSchema();
                                            // add for merging later
                                            cdcMergeDS = transformDF;
                                            cdcMergeDS.count();
                                            //
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
                                            source_data = sparkSession.read().parquet(path).alias("ds");
                                            System.out.println("hiihihi");
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
                                                WindowSpec w1 = Window.partitionBy(col(identity_key));
                                                transformDF.show(false);
                                                // union old and new data
                                                try {
                                                    String[] source_data_cols = source_data.columns();
                                                    for (int index = 0; index < source_data_cols.length; index++) {
                                                        source_data_cols[index] = source_data_cols[index].toUpperCase();
                                                    }
                                                    ArrayList<String> listColsSource = new ArrayList<>(Arrays.asList(source_data_cols));
                                                    Collections.sort(listCDCCols);
                                                    Collections.sort(listColsSource);
                                                    //
                                                    System.out.println("cdc size: " + listCDCCols.size());
                                                    System.out.println("source size: " + listColsSource.size());
                                                    //
                                                    ArrayList<String> mergeCols = new ArrayList<>();
                                                    // merge two rows
                                                    int indexCDC = 0;
                                                    int indexSource = 0;
                                                    while (indexCDC < listCDCCols.size() || indexSource < listColsSource.size()) {
                                                        //
                                                        int compareVal = listColsSource.get(indexSource).compareTo(listCDCCols.get(indexCDC));
                                                        //
                                                        if (compareVal == 0) {
                                                            mergeCols.add(listColsSource.get(indexSource));
                                                            indexCDC++;
                                                            indexSource++;
                                                        } else if (compareVal < 0) {
                                                            mergeCols.add(listColsSource.get(indexSource));
                                                            indexSource++;
                                                        } else {
                                                            mergeCols.add(listCDCCols.get(indexCDC));
                                                            indexCDC++;
                                                        }
                                                        //
                                                        if (indexCDC == listCDCCols.size() - 1) {
                                                            //
                                                            while (indexSource < listColsSource.size()) {
                                                                mergeCols.add(listColsSource.get(indexSource));
                                                                indexSource++;
                                                            }
                                                            break;
                                                        }
                                                        //
                                                        if (indexSource == listColsSource.size() - 1) {
                                                            //
                                                            while (indexCDC < listCDCCols.size()) {
                                                                mergeCols.add(listCDCCols.get(indexCDC));
                                                                indexCDC++;
                                                            }
                                                            break;
                                                        }

                                                    }
                                                    // debugging
                                                    System.out.println("cdc");
                                                    for (String col : listCDCCols) {
                                                        System.out.println(col);
                                                    }
                                                    System.out.println("source");
                                                    for (String col : listColsSource) {
                                                        System.out.println(col);
                                                    }
                                                    System.out.println("merge");
                                                    for (String col : mergeCols) {
                                                        System.out.println(col);
                                                    }
                                                    // generate new col
                                                    ArrayList<String> newSourceCols = new ArrayList<>();
                                                    ArrayList<String> newCDCCols = new ArrayList<>();
                                                    schema = schema.toUpperCase();
                                                    for (String col : mergeCols) {

                                                        int startIndex = schema.indexOf("\"" + col + ":") + col.length() + 2;
                                                        int endIndex = schema.indexOf(",", startIndex);
                                                        String type = schema.substring(startIndex, endIndex);
                                                        col = col.toUpperCase();
                                                        System.out.println(type);
                                                        if (listCDCCols.contains(col)) {
                                                            // if contain then remain
                                                            newCDCCols.add(col);
                                                        } else {
                                                            // else generate as null
                                                            newCDCCols.add(String.format("cast (null as %s) as %s", type, col));
                                                        }
                                                        //
                                                        if (!col.equalsIgnoreCase("OPERATION") && !col.equalsIgnoreCase("MODIFIED")) {
                                                            if (listColsSource.contains(col)) {
                                                                // if contain then remain
                                                                newSourceCols.add(col);
                                                            } else {
                                                                // else generate as null
                                                                newSourceCols.add(String.format("cast (null as %s) as %s", type, col));
                                                            }
                                                        }
                                                    }
                                                    // add new columns
                                                    System.out.println("before");
                                                    transformDF.show();
                                                    source_data = source_data.selectExpr(JavaConverters.asScalaIteratorConverter(newSourceCols.iterator()).asScala().toSeq());
                                                    source_data.printSchema();
                                                    source_data.write().mode("overwrite").parquet("/user/temp" + path);
                                                    sparkSession
                                                            .read().parquet("/user/temp" + path)
                                                            .write()
                                                            .mode("overwrite")
                                                            .parquet(path);
                                                    //
                                                    System.out.println("col cdc");
                                                    for (String col : newCDCCols) {
                                                        System.out.println(col);
                                                    }
                                                    System.out.println("col source");
                                                    for (String col : newSourceCols) {
                                                        System.out.println(col);
                                                    }
                                                    //
                                                    transformDF = transformDF.selectExpr(JavaConverters.asScalaIteratorConverter(newCDCCols.iterator()).asScala().toSeq());

                                                    transformDF.printSchema();
                                                    source_data.printSchema();
                                                } catch (Exception exception) {
                                                    exception.printStackTrace();
                                                }
                                                //
                                                source_data = sparkSession.read().parquet(path);
                                                source_data = source_data.withColumn("is_new", lit(0))
                                                        .unionByName(transformDF.drop("operation")
                                                                .withColumn("is_new", lit(1)))
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
//                                            Dataset<Row> tmp = null;
//                                            try {
//                                                sendLogs(8, "processing", "write data to temp location", log, row, connection);
//                                                source_data.write()
//                                                        .mode("overwrite")
//                                                        .parquet(String.format("/user/tmp/%s/%s", row.getAs("database"), row.getAs("table")));
//                                                tmp = sparkSession.read().parquet(String.format("/user/tmp/%s/%s", row.getAs("database"), row.getAs("table")));
//                                                sendLogs(8, "success", "done writing data to temp location", log, row, connection);
//                                            } catch (Exception exception) {
//                                                exception.printStackTrace();
//                                                String messageError = "Write to temp location failed !";
//                                                sendLogs(8, "fail", messageError, log, row, connection);
//                                                throw new Exception(messageError);
//                                            }
//                                            // move from location to main location
//                                            try {
//                                                sendLogs(9, "processing", "move from location to main location", log, row, connection);
//                                                tmp.write().mode("overwrite").parquet(path);
//                                                sendLogs(9, "success", "done moving from location to main location", log, row, connection);
//                                            } catch (Exception exception) {
//                                                exception.printStackTrace();
//                                                String messageError = "Fail moving from location to main location";
//                                                sendLogs(9, "success", messageError, log, row, connection);
//                                                throw new Exception(messageError);
//                                            }
                                            System.out.println("done");
                                        } else {
                                            //
                                            ArrayList<String> listPartitions = new ArrayList<>(Arrays.asList(partitionBy.split(",")));
                                            //
                                            try {
                                                String[] source_data_cols = source_data.columns();
                                                for (int index = 0; index < source_data_cols.length; index++) {
                                                    source_data_cols[index] = source_data_cols[index].toUpperCase();
                                                }
                                                ArrayList<String> listColsSource = new ArrayList<>(Arrays.asList(source_data_cols));
                                                Collections.sort(listCDCCols);
                                                Collections.sort(listColsSource);
                                                //
                                                System.out.println("cdc size: " + listCDCCols.size());
                                                System.out.println("source size: " + listColsSource.size());
                                                //
                                                ArrayList<String> mergeCols = new ArrayList<>();
                                                // merge two rows
                                                int indexCDC = 0;
                                                int indexSource = 0;
                                                while (indexCDC < listCDCCols.size() || indexSource < listColsSource.size()) {
                                                    //
                                                    int compareVal = listColsSource.get(indexSource).compareTo(listCDCCols.get(indexCDC));
                                                    //
                                                    if (compareVal == 0) {
                                                        mergeCols.add(listColsSource.get(indexSource));
                                                        indexCDC++;
                                                        indexSource++;
                                                    } else if (compareVal < 0) {
                                                        mergeCols.add(listColsSource.get(indexSource));
                                                        indexSource++;
                                                    } else {
                                                        mergeCols.add(listCDCCols.get(indexCDC));
                                                        indexCDC++;
                                                    }
                                                    //
                                                    if (indexCDC == listCDCCols.size() - 1) {
                                                        //
                                                        while (indexSource < listColsSource.size()) {
                                                            mergeCols.add(listColsSource.get(indexSource));
                                                            indexSource++;
                                                        }
                                                        break;
                                                    }
                                                    //
                                                    if (indexSource == listColsSource.size() - 1) {
                                                        //
                                                        while (indexCDC < listCDCCols.size()) {
                                                            mergeCols.add(listCDCCols.get(indexCDC));
                                                            indexCDC++;
                                                        }
                                                        break;
                                                    }

                                                }
                                                // debugging
                                                System.out.println("cdc");
                                                for (String col : listCDCCols) {
                                                    System.out.println(col);
                                                }
                                                System.out.println("source");
                                                for (String col : listColsSource) {
                                                    System.out.println(col);
                                                }
                                                System.out.println("merge");
                                                for (String col : mergeCols) {
                                                    System.out.println(col);
                                                }
                                                // generate new col
                                                ArrayList<String> newSourceCols = new ArrayList<>();
                                                ArrayList<String> newCDCCols = new ArrayList<>();
                                                schema = schema.toUpperCase();
                                                for (String col : mergeCols) {

                                                    int startIndex = schema.indexOf("\"" + col + ":") + col.length() + 2;
                                                    int endIndex = schema.indexOf(",", startIndex);
                                                    String type = schema.substring(startIndex, endIndex);
                                                    col = col.toUpperCase();
                                                    System.out.println(type);
                                                    if (listCDCCols.contains(col)) {
                                                        // if contain then remain
                                                        newCDCCols.add(col);
                                                    } else {
                                                        // else generate as null
                                                        newCDCCols.add(String.format("cast (null as %s) as %s", type, col));
                                                    }
                                                    //
                                                    if (!col.equalsIgnoreCase("OPERATION") && !col.equalsIgnoreCase("MODIFIED")) {
                                                        if (listColsSource.contains(col)) {
                                                            // if contain then remain
                                                            newSourceCols.add(col);
                                                        } else {
                                                            // else generate as null
                                                            newSourceCols.add(String.format("cast (null as %s) as %s", type, col));
                                                        }
                                                    }
                                                }
                                                newCDCCols.add("modified");
                                                // add new columns
                                                System.out.println("before");
                                                transformDF.show();
                                                source_data = source_data.selectExpr(JavaConverters.asScalaIteratorConverter(newSourceCols.iterator()).asScala().toSeq());
                                                source_data.printSchema();
                                                source_data.write().mode("overwrite").parquet("/user/temp" + path);
                                                sparkSession
                                                        .read().parquet("/user/temp" + path)
                                                        .write()
                                                        .mode("overwrite")
                                                        .partitionBy(JavaConverters.asScalaIteratorConverter(listPartitions.iterator()).asScala().toSeq())
                                                        .parquet(path);
                                                //
                                                System.out.println("col cdc");
                                                for (String col : newCDCCols) {
                                                    System.out.println(col);
                                                }
                                                System.out.println("col source");
                                                for (String col : newSourceCols) {
                                                    System.out.println(col);
                                                }
                                                //
                                                transformDF = transformDF.selectExpr(JavaConverters.asScalaIteratorConverter(newCDCCols.iterator()).asScala().toSeq());
                                                transformDF.show();
                                            } catch (Exception exception) {
                                                exception.printStackTrace();
                                            }
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
                                            // union new data arrived
                                            WindowSpec w1 = Window.partitionBy(row.<String>getAs("identity_key"));
                                            try {
                                                source_data = sparkSession.read().parquet(path).alias("ds");
                                                // we have to compare columns
                                                source_data = source_data.join(notAddDS, expr(joinCondition), "left_semi")
                                                        .select(source_data.col("*"))
                                                        .withColumn("modified", lit(null))
                                                        .withColumn("operation", lit(null));
                                                notAddDS.printSchema();
                                                source_data = source_data
                                                        .unionByName(notAddDS)
                                                        .withColumn("last____time", max("modified").over(w1))
                                                        .filter(expr("(modified = last____time or (modified is null and last____time is null)) and (operation <> 3 or operation is null)"))
                                                        .drop("modified", "operation");
                                                //
                                                sendLogs(8, "processing", "merge update/delete data", log, row, connection);
                                                if (source_data.count() > 0) {
                                                    // rewrite data updated and deleted
                                                    source_data.show();
                                                    source_data.write()
                                                            .mode(SaveMode.Overwrite)
                                                            .partitionBy(JavaConverters.asScalaIteratorConverter(listPartitions.iterator()).asScala().toSeq())
                                                            .parquet(path);
                                                }
                                                //
                                                sendLogs(8, "success", "done merging update/delete data", log, row, connection);
                                                sendLogs(9, "processing", "merge insert data", log, row, connection);
                                                if (transformDF.filter("operation = 1").count() > 0) {
                                                    // append new data inserted
                                                    transformDF.filter("operation = 1").write()
                                                            .mode(SaveMode.Append)
                                                            .partitionBy(JavaConverters.asScalaIteratorConverter(listPartitions.iterator()).asScala().toSeq())
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
                                        System.out.println("doneee");
                                        /*
                                         * get all merge request that having columns
                                         * convert columns
                                         * handle columns
                                         * write output
                                         * */
                                        System.out.println("starting to merge in merge table");
                                        String databaseAlias = getAlias(host, port, connection);
                                        String query = String.format("SELECT latest_metadata, merge_table_name from webservice_test.merge_requests " +
                                                "WHERE latest_metadata like ? and " +
                                                "latest_metadata like ? ");
                                        System.out.println(query);
                                        PreparedStatement prpStmt = connection
                                                .prepareStatement(query);
                                        prpStmt.setString(1, "%\"table\": \"" + table + "\"%");
                                        prpStmt.setString(2, "%\"database_alias\": \"" + databaseAlias + "\"%");
                                        ResultSet rs = prpStmt.executeQuery();
                                        while (rs.next()) {
                                            MergeRequest metadata = new ObjectMapper().readValue(rs.getString("latest_metadata"), MergeRequest.class);
                                            String mergeTable = rs.getString("merge_table_name");
                                            String mergePath = String.format("/user/merge_request/%s", mergeTable);
                                            //
                                            Dataset<Row> mergeSource = sparkSession.read().parquet(mergePath);
                                            ArrayList<String> mergeColumns = new ArrayList<String>(Arrays.asList(mergeSource.columns()));
                                            Dataset<Row> cdcDS = cdcMergeDS.drop("modified");
                                            // for making an action
                                            cdcDS.count();
                                            cdcDS = transformColumn(cdcDS, metadata, table, databaseAlias);
                                            ArrayList<String> cdcColumns = new ArrayList<String>(Arrays.asList(cdcDS.columns()));
                                            //
                                            //
                                            ArrayList<Column> partition = new ArrayList<>();

                                            for (MappingModel mm : metadata.getList_mapping()) {
                                                boolean isMergeExist = mergeColumns.contains(mm.getColName());
                                                boolean isColExist = cdcColumns.contains(mm.getColName());
                                                if (mm.getIs_unique() == 1 && isColExist && isMergeExist) {
                                                    partition.add(col(mm.getColName()));
                                                }
                                            }
                                            //
                                            ArrayList<StructField> listFields = new ArrayList<>(Arrays.asList(mergeSource.schema().fields()));
                                            HashMap<String, String> fieldHM = new HashMap<>();
                                            //
                                            for (int index = 0; index < listFields.size(); index++) {
                                                fieldHM.put(listFields.get(index).name().toUpperCase(), listFields.get(index).dataType().typeName());
                                                fieldHM.put(listFields.get(index).name().toLowerCase(), listFields.get(index).dataType().typeName());
                                            }
                                            //
                                            ArrayList<StructField> listCDCFields = new ArrayList<>(Arrays.asList(cdcDS.schema().fields()));
                                            HashMap<String, String> fieldCDCHM = new HashMap<>();
                                            for (int index = 0; index < listCDCFields.size(); index++) {
                                                fieldCDCHM.put(listCDCFields.get(index).name().toUpperCase(), listCDCFields.get(index).dataType().typeName());
                                                fieldCDCHM.put(listCDCFields.get(index).name().toLowerCase(), listCDCFields.get(index).dataType().typeName());
                                            }
                                            //
                                            mergeColumns.replaceAll(String::toUpperCase);
                                            cdcColumns.replaceAll(String::toUpperCase);
                                            System.out.println(mergeColumns);
                                            System.out.println(cdcColumns);
                                            for (String col : mergeColumns) {
                                                System.out.println(col);
                                                System.out.println("-------------");
                                                if (!cdcColumns.contains(col)) {
                                                    cdcDS = cdcDS.withColumn(col, expr(String.format("cast (null as %s) as %s"
                                                            , fieldHM.get(col), col)));
                                                } else {
                                                    if (!fieldHM.get(col.toUpperCase()).equalsIgnoreCase(fieldCDCHM.get(col.toUpperCase()))) {
                                                        // if different type then convert all to string
                                                        cdcDS = cdcDS.withColumn(col, expr(String.format("cast (%s as string)", col)));
                                                        mergeSource = mergeSource.withColumn(col, expr(String.format("cast (%s as string)", col)));
                                                    }
                                                }
                                            }
                                            //
                                            String randomCol = "col_1231223";
                                            WindowSpec windowSpec = Window.partitionBy(JavaConverters.asScalaIteratorConverter(partition.iterator()).asScala().toSeq());
                                            mergeSource = mergeSource
                                                    .withColumn(randomCol, lit(0))
                                                    .withColumn("operation", lit(null))
                                                    .unionByName(cdcDS.withColumn(randomCol, lit(1)))
                                                    .withColumn("is_max_12133213", max(randomCol).over(windowSpec))
                                                    .filter(expr(String.format("%s = is_max_12133213", randomCol)))
                                                    .filter("operation <> 3 or operation is null")
                                                    .drop(randomCol, "is_max_12133213");
                                            //
                                            mergeSource.write().mode("overwrite").parquet("/temp" + mergePath);
                                            //
                                            sparkSession.read().parquet("/temp" + mergePath).write().mode("overwrite").parquet(mergePath);
                                            //
                                            System.out.println("done merging");
                                        }
                                        //
                                    }
                                } catch (Exception exception) {
                                    // retrying
                                    exception.printStackTrace();
                                }
                            }
                            // processing merge request when new table is assigned
                            try {
                                requestedDataset.show();
                                Dataset<Row> mergeDS = requestedDataset.filter("topic = 'merge-request'")
                                        .withColumn("extract", from_json(col("value").cast("string"), merge_schema))
                                        .select(col("extract.*"));
                                mergeDS.show();
                                //
                                List<Row> list_merge_request = mergeDS.collectAsList();
                                for (Row row : list_merge_request) {
                                    String databaseType = row.getAs("databaseType");
                                    String host = row.getAs("host");
                                    String port = row.getAs("port");
                                    String database = row.getAs("database");
                                    String username = row.getAs("username");
                                    String table = row.getAs("table");
                                    String mergeTable = row.getAs("mergeTable");
                                    String path = "";
                                    String mergePath = String.format("/user/merge_request/%s", mergeTable);
                                    if (!databaseType.equals("oracle")) {
                                        path = String.format("/user/%s/%s/%s/%s/%s/", databaseType, host, port, database, table);
                                    } else {
                                        path = String.format("/user/%s/%s/%s/%s/%s/", databaseType, host, port, username, table);
                                    }
                                    // get metadata
                                    PreparedStatement prpStmt = connection.prepareStatement("SELECT * FROM webservice_test.merge_requests where merge_table_name = ?");
                                    prpStmt.setString(1, mergeTable);
                                    ResultSet rs = prpStmt.executeQuery();
                                    MergeRequest new_metadata = null;
                                    while (rs.next()) {
                                        String latest_metadata = rs.getString("latest_metadata");
                                        new_metadata = new ObjectMapper().readValue(latest_metadata, MergeRequest.class);
                                        break;
                                    }
                                    Dataset<Row> ds = sparkSession.read().parquet(path);
                                    ds = transformColumn(ds, new_metadata, table, getAlias(host, port, connection));
                                    //
                                    ArrayList<String> newColsName = new ArrayList<>(Arrays.asList(ds.columns()));
                                    ArrayList<String> commonCols = new ArrayList<>();
                                    //

                                    //
                                    if (!checkPath(mergePath)) {
                                        // if there is exist merge request
                                        ds.write().mode("overwrite").parquet(mergePath);
                                        System.out.println("done new");
                                    } else {
                                        // otherwise
                                        //
                                        Dataset<Row> mergeData = sparkSession.read().parquet(mergePath).alias("ms");
                                        ArrayList<String> mergeCols = new ArrayList<>(Arrays.asList(mergeData.columns()));
                                        String joinCondition = "";
                                        //
                                        System.out.println(newColsName);
                                        System.out.println(mergeCols);
                                        for (MappingModel mm : new_metadata.getList_mapping()) {
                                            boolean isMergeExist = mergeCols.contains(mm.getColName());
                                            boolean isColExist = newColsName.contains(mm.getColName());
                                            if (mm.getIs_unique() == 1 && isColExist && isMergeExist) {
                                                joinCondition += String.format("ms.%s = nc.%s_temp_12345654321 and ", mm.getColName(), mm.getColName());
                                            }
                                            if (isColExist) {
                                                ds = ds.withColumnRenamed(mm.getColName(), mm.getColName() + "_temp_12345654321");
                                                commonCols.add(mm.getColName());
                                            }
                                        }
                                        // remove the last 'and'
                                        joinCondition = joinCondition.substring(0, joinCondition.length() - 4);
                                        System.out.println(String.format("join conditions: %s", joinCondition));

                                        ds = ds.alias("nc");
                                        mergeData = mergeData.join(ds, expr(joinCondition), "full")
                                                .select(col("ms.*"), col("nc.*"));
                                        System.out.println("join");
                                        for (String cm : commonCols) {
                                            boolean isMergeExist = mergeCols.contains(cm);
                                            boolean isColExist = newColsName.contains(cm);
                                            if (isColExist && isMergeExist) {
                                                mergeData = mergeData
                                                        .withColumn(cm + "_temp_12345654321", expr(String.format("cast (%s as string)", cm + "_temp_12345654321")))
                                                        .withColumn(cm, expr(String.format("cast (%s as string)", cm)))
                                                        .withColumn(cm, coalesce(col(cm + "_temp_12345654321"), col(cm)))
                                                        .drop(cm + "_temp_12345654321");
                                            } else {
                                                mergeData = mergeData
                                                        .withColumn(cm, col(cm + "_temp_12345654321"))
                                                        .drop(cm + "_temp_12345654321");
                                            }
                                        }
                                        mergeData.write().mode("overwrite").parquet("/temp" + mergePath);
                                        sparkSession.read().parquet("/temp" + mergePath).write().mode("overwrite").parquet(mergePath);
                                        System.out.println("done");
                                    }
                                }

                            } catch (Exception exception) {
                                exception.printStackTrace();
                            }
                            //
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

    private static Dataset<Row> transformColumn(Dataset<Row> inputDS, MergeRequest mr, String table, String alias) {
        String fullTable = alias + "." + table;
        System.out.println(String.format("Alias : %s - Table: %s", alias, fullTable));
        ArrayList<String> listCol = new ArrayList<>(Arrays.asList(inputDS.columns()));
        listCol.replaceAll(String::toLowerCase);
        //
        listCol.remove("operation");
        //
        HashMap<String, String> mappingCol = new HashMap<>();
        //
        for (MappingModel mp : mr.getList_mapping()) {
            System.out.println("common: " + mp.getColName());
            System.out.println("fulltable: " + fullTable);
            for (String col : mp.getListCol()) {
                System.out.println("col: " + col);
                int index = col.indexOf(fullTable);
                System.out.println("index: " + index);
                // handle if that column is deleted
                if (index >= 0) {
                    String currentCol = col.substring(index + fullTable.length() + 1);
                    // handle if that column is deleted
                    if (listCol.contains(currentCol)) {
                        System.out.println(String.format("%s : %s", fullTable, currentCol));
                        try {
//                        inputDS = inputDS.withColumnRenamed(currentCol, mp.getColName());
                            mappingCol.put(currentCol, mp.getColName());
                            listCol.remove(currentCol);
                            System.out.println(listCol);
                        } catch (Exception exception) {
                            exception.printStackTrace();
                        }
                    }
                }
            }
            System.out.println("--------------------");
        }
        //
        table = table.replaceAll("[.]", "_");
        for (String remainCol : listCol) {
            inputDS = inputDS.withColumnRenamed(remainCol, String.format("%s_%s_%s", remainCol, table, alias));
        }

        for (String key : mappingCol.keySet()) {
            inputDS = inputDS.withColumnRenamed(key, mappingCol.get(key));
        }
        //
        return inputDS;
    }

    private static String getAlias(String host, String port, Connection connection) throws SQLException {
        String query = "" +
                "SELECT distinct alias FROM webservice_test.database_infos di\n" +
                "INNER JOIN\n" +
                "webservice_test.server_infos si\n" +
                "on di.server_info_id = si.id where `server_host` = ? and port = ?";
        PreparedStatement prpStmt = connection.prepareStatement(query);
        prpStmt.setString(1, host);
        prpStmt.setString(2, port);
        ResultSet rs = prpStmt.executeQuery();
        while (rs.next()) {
            return rs.getString("alias");
        }
        return null;
    }

    private static String getDefaultKeys(Connection connection, String host, String port, String database, String table) throws SQLException {
        String query = "" +
                "select default_key from \n" +
                "webservice_test.`tables` tbl\n" +
                "inner join webservice_test.database_infos di\n" +
                "inner join webservice_test.server_infos si\n" +
                "on tbl.database_info_id = di.id and di.server_info_id = si.id\n" +
                "where `table_name` = ? and database_name = ? and server_host = ? and port = ?;";
        PreparedStatement prpStmt = connection.prepareStatement(query);
        prpStmt.setString(1, table);
        prpStmt.setString(2, database);
        prpStmt.setString(3, host);
        prpStmt.setString(4, port);
        ResultSet rs = prpStmt.executeQuery();
        while (rs.next()) {
            return rs.getString("default_key");
        }
        return null;
    }

    private static Boolean checkPath(String uri) {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            FileStatus[] fileStatuses = fs.globStatus(new Path(uri));
            if (fileStatuses == null) return false;
            // Check if folder exists at the given location
            return fileStatuses.length > 0;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
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
