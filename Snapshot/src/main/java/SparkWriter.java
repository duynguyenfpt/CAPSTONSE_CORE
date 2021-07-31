import models.LogModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Objects;

import static org.apache.spark.sql.functions.*;

import utils.*;

public class SparkWriter {
    public static void main(String[] args) throws SQLException {
        // Snapshot
        String jobID = "";
        LogModel log = null;
        //
        try {
            String database = args[0];
            String table = args[1];
            String username = args[2];
            String password = args[3];
            String host = args[4];
            String port = args[5];
            String partitionBy = args[6];
            jobID = args[7];
            String strID = args[8];
            String database_type = args[9];
//        System.out.println(partitionBy.equals(""));
//        System.out.println(partitionBy.equals(" "));
//        System.out.println(Objects.isNull(partitionBy));
            log = new LogModel(Integer.parseInt(jobID), Integer.parseInt(strID),
                    host, port, database, table, 1, "sync_all",
                    5, null, null);
            SparkSession sparkSession = SparkSession
                    .builder()
                    .appName(String.format("snapshot database  %s-%s-%s-%s to parquet", host, port, database, table))
                    .getOrCreate();

            sendLogs(5, "processing", "start snapshotting", log);

            String path = String.format("/user/test/%s/%s/", database, table);
            String url = String.format("jdbc:mysql://%s:%s/%s?useSSL=false&" +
                    "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC", host, port, database);
            String currentTime = LocalDateTime.now()
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
            Dataset<Row> input = sparkSession.read()
                    .format("jdbc")
                    .option("url", url)
                    .option("dbtable", table).option("user", username).option("password", password)
                    .load()
                    .withColumn("modified", lit(currentTime));

            input.show();
            System.out.println(path);

            if (!partitionBy.equals(" ")) {
                System.out.println("do this");
                input.write()
                        .mode("overwrite")
                        .partitionBy(partitionBy)
                        .parquet(path);
            } else {
                System.out.println("do that");
                input.write()
                        .mode("overwrite")
                        .parquet(path);
            }
            sendLogs(5, "success", "success snapshotting", log);
        } catch (Exception exception) {
            exception.printStackTrace();
            sendLogs(5, "failed", "failed snapshotting", log);
            System.out.println(exception.getMessage());
        }
    }

    public static void sendLogs(int step, String status, String message, LogModel log) {
        log.setStep(step);
        log.setStatus(status);
        log.setMessage(message);
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        Date date = new Date();
        log.setCreate_time(dateFormat.format(date));
        if (status.equals("success")) {
            log.setStatusOrder(3);
        } else if (status.equals("fail")) {
            log.setStatusOrder(2);
        } else if (status.equals("processing")) {
            log.setStatusOrder(1);
        }
        sqlUtils.logProducer("localhost:9092", "jobs_log", log);
    }
}
