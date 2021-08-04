import models.LogModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
            log = new LogModel(Integer.parseInt(jobID), Integer.parseInt(strID),
                    host, port, database, table, 1, "sync_all",
                    5, null, null);
            //
            Connection configConnection = sqlUtils.getConnection(sqlUtils.getConnectionString("localhost", "3306",
                    "cdc", "duynt", "Capstone123@"));
            String sid = sqlUtils.getSID(host, port, configConnection);
            //
            SparkSession sparkSession = SparkSession
                    .builder()
                    .appName(String.format("snapshot database  %s-%s-%s-%s to parquet", host, port, database, table))
                    .getOrCreate();

            sendLogs(5, "processing", "start snapshotting", log);
            String path = "";
            if (!database_type.equalsIgnoreCase("oracle")) {
                path = String.format("/user/%s/%s/%s/%s/%s/", database_type, host, port, database, table);
            } else {
                path = String.format("/user/%s/%s/%s/%s/%s/", database_type, host, port, username, table);
            }

            String url = "";

            if (database_type.equalsIgnoreCase("mysql") || database_type.equalsIgnoreCase("postgresql")) {
                url = String.format("jdbc:%s://%s:%s/%s?user=%s&password=%s&useSSL=false&characterEncoding=utf-8&allowPublicKeyRetrieval=true"
                        , database_type, host, port, database, username, password);
            } else if (database_type.equals("oracle")) {
                url = String.format("jdbc:oracle:thin:%s/%s@%s:%s:%s", username, password, host, port, sid);
            }
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

    public static String getSID(String host, String port, Connection connection) throws SQLException {
        String query = "SELECT sid from webservice_test.database_infos di\n" +
                "inner join webservice_test.server_infos si\n" +
                "on si.deleted = 0 and di.deleted = 0 and di.server_info_id = si.id\n" +
                "where si.server_host = ? and port = ? ;";
        PreparedStatement prpStmt = connection.prepareStatement(query);
        prpStmt.setString(1, host);
        prpStmt.setString(2, port);
        ResultSet rs = prpStmt.executeQuery();
        while (rs.next()) {
            return rs.getString("sid");
        }
        return null;
    }
}
