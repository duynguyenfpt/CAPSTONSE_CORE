package utils;

import com.google.gson.Gson;
import models.LogModel;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.*;
import java.util.Properties;

public class sqlUtils {
    public static String getConnectionString(String host, String port, String db, String userName, String password) {
//        return String.format("jdbc:mysql://%s:%s/%s?user=%s&password=%s&useSSL=false&characterEncoding=utf-8&verifyServerCertificate=false&autoReconnect=true", host, port, db, userName, password);
        return String.format("jdbc:mysql://%s:%s/%s?user=%s&password=%s&useSSL=false&characterEncoding=utf-8&allowPublicKeyRetrieval=true", host, port, db, userName, password);
    }

    //
    public static Connection getConnection(String dbURL) {
        Connection conn = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(dbURL);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return conn;
    }

    //
    public static void insertJobLog(String job_id, int step_id, String step_name, int status) throws SQLException {
        Connection connection = getConnection(getConnectionString("localhost", "3306",
                "synchronization", "duynt", "Capstone123@"));
        String command = "Insert into synchronization.job_log(job_id,step_id,step_name,status,number_steps,status_name) " +
                "values (?,?,?,?,?,?)";
        int numberSteps = 5;
        String statusName = "Success";
        if (status == 0) {
            statusName = "Failed";
        }

        PreparedStatement prpStmt = connection.prepareStatement(command);
        prpStmt.setString(1, job_id);
        prpStmt.setInt(2, step_id);
        prpStmt.setString(3, step_name);
        prpStmt.setInt(4, status);
        prpStmt.setInt(5, numberSteps);
        prpStmt.setString(6, statusName);
        prpStmt.executeUpdate();
    }

    public static void logProducer(String kafkaCluster, String kafkaTopic, LogModel log) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //If the request fails, the producer can automatically retry,
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //Reduce the no of requests less than 0
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        Gson gson = new Gson();
        producer.send(new ProducerRecord<String, String>(kafkaTopic, log.getHost() + "-" + log.getPort() + "-"
                + log.getDatabase_name() + "-" + log.getTable_name(), gson.toJson(log)));
        producer.close();
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
