package utils.db_utils;

import java.sql.*;
import java.util.ArrayList;

import com.google.gson.Gson;
import models.CDCModel;
import org.apache.kafka.common.protocol.types.Field;

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
                                       String table, int operationType, String cdcDB, String cdcTable) {
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
                " INSERT INTO %s.%s\n" +
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
                database, table, cdcDB, cdcTable, host, port, database, table, operationType, valueDetail, gson.toJson(fields));
    }

    public static Integer getOffsets(Connection connection, String db, String host, String port) {
        String query = "SELECT offsets from cdc.test_offsets where `database` = ? and `host` = ? and `port` = ?";
        PreparedStatement prpStmt = null;
        try {
            prpStmt = connection.prepareStatement(query);
            prpStmt.setString(1, db);
            prpStmt.setString(2, host);
            prpStmt.setString(3, port);
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

    public static ArrayList<CDCModel> getCDCs(Connection connection, int offsets, int max_id) {
        ArrayList<CDCModel> listCDCs = new ArrayList<CDCModel>();
        String getDataQuery = "SELECT * FROM cdc.test_cdc_detail " +
                "where id > ? and id <= ? " +
                "order by table_name ";
        try {
            PreparedStatement prpStmt = connection.prepareStatement(getDataQuery);
            prpStmt.setInt(1, offsets);
            prpStmt.setInt(2, max_id);
            ResultSet rs = prpStmt.executeQuery();
            while (rs.next()) {
                listCDCs.add(
                        new CDCModel(rs.getInt("id"), rs.getString("database_url"), rs.getString("database_port"),
                                rs.getString("database_name"), rs.getString("table_name"), rs.getString("schema"),
                                rs.getString("value"), rs.getInt("operation"), rs.getDate("created").getTime())
                );
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        return listCDCs;
    }

    public static void updateOffset(Connection connection, String db, String host, String port, int offsets) {
        String query = "update cdc.test_offsets set offsets = ? where `database` = ? and `host` = ? and `port` = ? ";
        try {
            PreparedStatement prpStmt = connection.prepareStatement(query);
            prpStmt.setInt(1, offsets);
            prpStmt.setString(2, db);
            prpStmt.setString(3, host);
            prpStmt.setString(4, port);
            prpStmt.executeUpdate();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
    }

    public static void initOffset(Connection connection, String db, String host, String port) throws SQLException {
        // must check exist ?
        // if not then update rather than insert
        System.out.println("init offset");
        //
        String queryCheck = "select * from cdc.`offsets` where database_host = ? and database_port = ? and database_name = ?";
        PreparedStatement prpCheck = connection.prepareStatement(queryCheck);
        prpCheck.setString(1, host);
        prpCheck.setString(2, port);
        prpCheck.setString(3, db);
        ResultSet rs = prpCheck.executeQuery();
        if (!rs.next()) {
            //
            String query = "insert into " +
                    "cdc.`offsets`(`database_host`,`database_port`,`database_name`,`offsets`) values (?,?,?,?)";
            PreparedStatement prpStmt = connection.prepareStatement(query);
            prpStmt.setString(1, host);
            prpStmt.setString(2, port);
            prpStmt.setString(3, db);
            prpStmt.setInt(4, 0);
            prpStmt.executeUpdate();
        } else {
            String query = "update " +
                    "cdc.`offsets` set `offsets` = 0 where database_host = ? and database_port = ? and database_name = ? ";
            PreparedStatement prpStmt = connection.prepareStatement(query);
            prpStmt.setString(1, host);
            prpStmt.setString(2, port);
            prpStmt.setString(3, db);
            prpStmt.executeUpdate();
        }
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

    public static void getTableInfo(String host, String port, String database, String tableName) {

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

    public static void updateActive(String host, String port, String database
            , String tableName, Connection connection) throws SQLException {
        String updateQuery = String.format("UPDATE cdc.table_monitor SET is_active = b'0' and is_ready = b'0' " +
                "WHERE host = ? and port = ? and `database` = ? and `table` = ?");
        PreparedStatement prpStmt = connection.prepareStatement(updateQuery);
        prpStmt.setString(1, host);
        prpStmt.setString(2, port);
        prpStmt.setString(3, database);
        prpStmt.setString(4, tableName);
        prpStmt.executeUpdate();
    }

    public static void resetMonitor(String host, String port, String database,
                                    String table, Connection connection) throws SQLException {
        String queryCheck = "SELECT * FROM cdc.table_monitor Where `host` = ? and `port` = ? and `database` = ? and `table` = ?";
        PreparedStatement prpStmt = connection.prepareStatement(queryCheck);
        prpStmt.setString(1, host);
        prpStmt.setString(2, port);
        prpStmt.setString(3, database);
        prpStmt.setString(4, table);
        ResultSet rs = prpStmt.executeQuery();
        if (rs.next()) {
            updateActive(host, port, database, table, connection);
        } else {
            String insetQuery = "insert into " +
                    "cdc.`table_monitor`(`host`,`port`,`database`,`table`, `is_active`,`is_ready`) values (?,?,?,?,?,?)";
            PreparedStatement insertStmt = connection.prepareStatement(insetQuery);
            insertStmt.setString(1, host);
            insertStmt.setString(2, port);
            insertStmt.setString(3, database);
            insertStmt.setString(4, table);
            insertStmt.setInt(5, 0);
            insertStmt.setInt(6, 0);
            insertStmt.executeUpdate();
        }
    }
}
