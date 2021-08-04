import com.google.gson.Gson;
import models.LogModel;
import models.TableMonitor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import oracle.jdbc.driver.OracleDriver;

import java.sql.*;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Properties;

public class sqlUtils {
    public static String getConnectionString(String host, String port, String db, String userName, String password) {
//        return String.format("jdbc:mysql://%s:%s/%s?user=%s&password=%s&useSSL=false&characterEncoding=utf-8&verifyServerCertificate=false&autoReconnect=true", host, port, db, userName, password);
        return String.format("jdbc:mysql://%s:%s/%s?user=%s&password=%s&useSSL=false&characterEncoding=utf-8&allowPublicKeyRetrieval=true", host, port, db, userName, password);
    }

    public static String getConnectionString(String databaseType, String host, String port, String db, String userName, String password) {
//        return String.format("jdbc:mysql://%s:%s/%s?user=%s&password=%s&useSSL=false&characterEncoding=utf-8&verifyServerCertificate=false&autoReconnect=true", host, port, db, userName, password);
        return String.format("jdbc:%s://%s:%s/%s?user=%s&password=%s&useSSL=false&characterEncoding=utf-8&allowPublicKeyRetrieval=true", databaseType, host, port, db, userName, password);
    }

    public static Connection getConnectionOracle(String username, String password, String host, String port, String SID) {
        Connection conn = null;
        try {
//            Class.forName("com.mysql.cj.jdbc.Driver");
            Class.forName("oracle.jdbc.driver.OracleDriver");
            conn = DriverManager.getConnection(String.format("jdbc:oracle:thin:%s/%s@%s:%s:%s", username, password, host, port, SID));
//            conn = DriverManager.getConnection(String.format("jdbc:oracle:thin:@%s:%s:%s", host, port, SID), username, password);
            System.out.println("connected successfully");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return conn;
    }

    //
    public static Connection getConnection(String dbURL) {
        Connection conn = null;
        try {
//            Class.forName("com.mysql.cj.jdbc.Driver");
//            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(dbURL);
            System.out.println("connected successfully");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return conn;
    }

    public static ArrayList<String> getListFields(Connection connection, String tableName) {
        ArrayList<String> results = new ArrayList<String>();
        String describeQuery = "DESCRIBE " + tableName;
        try {
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(describeQuery);
            while (rs.next()) {
                results.add(rs.getString("Field").toLowerCase() + ":" + rs.getString("Type"));
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        return results;
    }

    public static String createTrigger(Connection connection, String host, String port, String database,
                                       String table, int operationType) {
        // set operation
        String operation = "INSERT";
        if (operationType == 2) {
            operation = "UPDATE";
        } else if (operationType == 3) {
            operation = "DELETE";
        }
        //
        //
        String triggerTemplate = "" +
                "CREATE TRIGGER test_after_%s_%s_%s\n" +
                "    AFTER %s ON %s.%s\n" +
                "    FOR EACH ROW\n" +
                " INSERT INTO cdc.test_cdc_detail\n" +
                " SET\n" +
                "  `database_url` = '%s',\n" +
                "  `database_port` = '%s',\n" +
                "  `database_name` = '%s',\n" +
                "  `table_name` = '%s',\n" +
                "  `operation` =  %d,\n" +
                "  `value` = %s,\n" +
                "  `schema` = '%s'";
        //
        String valueDetail = "";
        // convert fields to json
        ArrayList<String> fields = getListFields(connection, table);
        Gson gson = new Gson();
        valueDetail = "concat('{',";
        //
        int count = 1;
        for (String field_value : fields) {
            String field = field_value.substring(0, field_value.indexOf(":"));
            if (operationType != 3) {
                valueDetail += String.format("'\"%s\":','\"',new.%s,'\"'", field, field);
            } else {
                valueDetail += String.format("'\"%s\":','\"',old.%s,'\"'", field, field);
            }
            if (count < fields.size()) {
                valueDetail += ",',',";
            } else {
                valueDetail += ",'}')";
            }
            count++;
        }
        ;
        return String.format(triggerTemplate, database, table, operation, operation,
                database, table, host, port, database, table, operationType, valueDetail, gson.toJson(fields));
    }

    public static Integer getOffsets(Connection connection, String db, String table) {
        String query = "SELECT offsets from cdc.test_offsets where `database` = ? and `table` = ?";
        PreparedStatement prpStmt = null;
        try {
            prpStmt = connection.prepareStatement(query);
            prpStmt.setString(1, db);
            prpStmt.setString(2, table);
            ResultSet rs = prpStmt.executeQuery();
            while (rs.next()) {
                return rs.getInt("offsets");
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        return null;
    }

    public static Integer getLatestID(Connection connection) {
        String query = "SELECT max(id) as max_id from cdc.test_cdc_detail";
        try {
            PreparedStatement prpStmt = connection.prepareStatement(query);
            ResultSet rs = prpStmt.executeQuery();
            while (rs.next()) {
                return rs.getInt("max_id");
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        return null;
    }

    public static void updateOffset(Connection connection, String host, String port, String db, String table, int offsets) {
        String query = "update cdc.table_monitor set latest_offset = ? where " +
                "`database` = ? and `table` = ? and host = ? and port = ? ";
        try {
            PreparedStatement prpStmt = connection.prepareStatement(query);
            prpStmt.setInt(1, offsets);
            prpStmt.setString(2, db);
            prpStmt.setString(3, table);
            prpStmt.setString(4, host);
            prpStmt.setString(5, port);
            prpStmt.executeUpdate();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
    }

    public static void updateReady(String host, String port, String database
            , String tableName, Connection connection, int readiness) throws SQLException {
        String updateQuery = String.format("UPDATE cdc.table_monitor SET is_ready = b'%d' " +
                "WHERE host = ? and port = ? and `database` = ? and `table` = ?", readiness);
        PreparedStatement prpStmt = connection.prepareStatement(updateQuery);
        prpStmt.setString(1, host);
        prpStmt.setString(2, port);
        prpStmt.setString(3, database);
        prpStmt.setString(4, tableName);
        prpStmt.executeUpdate();
    }

    public static int getLatestId(String host, String port, String database
            , String tableName, Connection connection) throws SQLException {

        String query = "SELECT MAX(ID) as latest_id from `cdc_4912929__cdc`.cdc_detail\n" +
                "Where database_url = ? and database_port = ? \n" +
                "and database_name = ? and `table_name` = ?";
        PreparedStatement prpStmt = connection.prepareStatement(query);
        prpStmt.setString(1, host);
        prpStmt.setString(2, port);
        prpStmt.setString(3, database);
        prpStmt.setString(4, tableName);

        ResultSet resultSet = prpStmt.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt("latest_id");
        }
        return 0;
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

    public static void applicationProducer(String kafkaCluster, String kafkaTopic, TableMonitor monitorModel) {
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
        System.out.println(kafkaTopic);
        producer.send(new ProducerRecord<String, String>(kafkaTopic, monitorModel.getServer_host() + "-" + monitorModel.getPort()
                + "-" + monitorModel.getDatabase_type() + "-" + monitorModel.getTable(), gson.toJson(monitorModel)));
        System.out.println("producing: " + gson.toJson(monitorModel));
        producer.close();
        System.out.println("DONE");
    }
}
