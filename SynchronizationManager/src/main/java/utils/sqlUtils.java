package utils;

import com.google.gson.Gson;
import models.LogModel;
import models.QueryModel;
import models.ReadinessModel;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.*;
import java.util.ArrayList;
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
        System.out.println("connected");
        return conn;
    }

    //
    public static void insertJobLog(Connection connection, String job_id, int step_id, String step_name, int status) throws SQLException {
        String command = "Insert into synchronization.job_log(job_id,step_id,step_name,status) " +
                "values (?,?,?,?)";
        PreparedStatement prpStmt = connection.prepareStatement(command);
        prpStmt.setString(1, job_id);
        prpStmt.setInt(2, step_id);
        prpStmt.setString(3, step_name);
        prpStmt.setInt(4, status);
        prpStmt.executeUpdate();
    }

    public static void updateReady(String host, String port, String database
            , String tableName, Connection connection, int readiness) {
        System.out.println(host);
        System.out.println(port);
        System.out.println(database);
        System.out.println(tableName);
        String updateQuery = String.format("UPDATE cdc.table_monitor SET is_active = b'%d' " +
                "WHERE host = ? and port = ? and `database` = ? and `table` = ?", readiness);
        PreparedStatement prpStmt = null;
        try {
            prpStmt = connection.prepareStatement(updateQuery);
            prpStmt.setString(1, host);
            prpStmt.setString(2, port);
            prpStmt.setString(3, database);
            prpStmt.setString(4, tableName);
            prpStmt.executeUpdate();
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }

    public static void updateReady(ArrayList<ReadinessModel> listRM, Connection connection, int readiness) throws SQLException {
        String host = "(";
        String port = "(";
        String tableName = "(";
        String database = "(";
        //
        for (int index = 0; index < listRM.size(); index++) {
            host = host + String.format("'%s'", listRM.get(index).getHost());
            port = host + String.format("'%s'", listRM.get(index).getPort());
            database = host + String.format("'%s'", listRM.get(index).getDatabase());
            tableName = host + String.format("'%s'", listRM.get(index).getTable());
            if (index < listRM.size() - 1) {
                host = host + ",";
                port = port + ",";
                database = database + ",";
                tableName = tableName + ",";
            }
        }
        //
        String updateQuery = String.format("UPDATE cdc.table_monitor SET is_active = b'%d' " +
                "WHERE host in ? and port in ? and `database` in ? and `table` in ?", readiness);
        PreparedStatement prpStmt = connection.prepareStatement(updateQuery);
        prpStmt.setString(1, host);
        prpStmt.setString(2, port);
        prpStmt.setString(3, database);
        prpStmt.setString(4, tableName);
        prpStmt.executeUpdate();
    }

    public static void updateIsProcess(Connection connection, int strID) throws SQLException {
        String updateQuery = "update webservice_test.sync_table_requests set is_process = b'1' where id = ?";
        PreparedStatement prpStmt = connection.prepareStatement(updateQuery);
        prpStmt.setInt(1, strID);
        prpStmt.executeUpdate();
    }

    public static ArrayList<ReadinessModel> checkRemainRequest(Connection connection, int jobID) throws SQLException {
        String checkQuery = "" +
                "SELECT si.server_host,di.port,di.database_name,tbls.`table_name`,\n" +
                "jobs.id as job_id, str.is_process,str.id as str_id,req.id as req_id FROM \n" +
                "(SELECT * FROM webservice_test.jobs) as jobs\n" +
                "INNER JOIN\n" +
                "(select is_all,id,request_id,partition_by,table_id,is_process, null as query,from_date,to_date from webservice_test.sync_table_requests where 1 = 1 \n" +
                ") str \n" +
                "INNER JOIN\n" +
                "webservice_test.tables tbls\n" +
                "INNER JOIN\n" +
                "webservice_test.database_infos as di\n" +
                "INNER JOIN\n" +
                "webservice_test.server_infos as si\n" +
                "INNER JOIN\n" +
                "webservice_test.request as req\n" +
                "on jobs.request_id = str.request_id\n" +
                "and jobs.request_id = req.id\n" +
                "and (str.table_id = tbls.id and si.id = di.server_info_id and tbls.database_info_id = di.id)\n" +
                "and jobs.id = ?";
        ArrayList<ReadinessModel> result = new ArrayList<>();
        PreparedStatement prpStmt = connection.prepareStatement(checkQuery);
        prpStmt.setInt(1, jobID);
        ResultSet rs = prpStmt.executeQuery();
        while (rs.next()) {
            ReadinessModel rm = new ReadinessModel();
            rm.setHost(rs.getString("server_host"));
            rm.setPort(rs.getString("port"));
            rm.setDatabase(rs.getString("database_name"));
            rm.setTable(rs.getString("table_name"));
            rm.setIsProcess(rs.getInt("is_process"));
            result.add(rm);
        }
        return result;
    }

    public static void updateJobStatus(Connection connection, int jobID, String status) throws SQLException {
        String updateQuery = "UPDATE `webservice_test`.`jobs` SET `status` = ? WHERE `id` = ?;";
        PreparedStatement preparedStatement = connection.prepareStatement(updateQuery);
        preparedStatement.setString(1, status);
        preparedStatement.setInt(2, jobID);
        preparedStatement.executeUpdate();
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

    //
    public static void requestProducer(String kafkaCluster, String kafkaTopic, QueryModel queryModel) {
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
        producer.send(new ProducerRecord<String, String>(kafkaTopic,
                queryModel.getRequestId() + "-" + queryModel.getJobId(), gson.toJson(queryModel)));
        producer.close();
    }
    //
}
