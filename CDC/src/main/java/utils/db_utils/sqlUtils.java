package utils.db_utils;

import java.sql.*;
import java.util.ArrayList;

import com.google.gson.Gson;
import models.CDCModel;

public class sqlUtils {
    public static String getConnectionString(String host, String port, String db, String userName, String password) {
//        return String.format("jdbc:mysql://%s:%s/%s?user=%s&password=%s&useSSL=false&characterEncoding=utf-8&verifyServerCertificate=false&autoReconnect=true", host, port, db, userName, password);
        return String.format("jdbc:mysql://%s:%s/%s?user=%s&password=%s&useSSL=false&characterEncoding=utf-8", host, port, db, userName, password);
    }

    //
    public static Connection getConnection(String dbURL) {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(dbURL);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return conn;
    }

    public static ArrayList<String> getListFields(Connection connection, String tableName) {
        ArrayList<String> results = new ArrayList<>();
        String describeQuery = "DESCRIBE " + tableName;
        try {
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(describeQuery);
            while (rs.next()) {
                results.add(rs.getString("Field").toLowerCase());
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
        String triggerTemplate = """
                CREATE TRIGGER test_after_%s_%s_%s\s
                    AFTER %s ON %s.%s
                    FOR EACH ROW\s
                 INSERT INTO cdc.test_cdc_detail
                 SET\s
                  `database_url` = '%s',
                  `database_port` = '%s',
                  `database_name` = '%s',
                  `table_name` = '%s',
                  `operation` =  %d,
                  `value` = %s,
                  `schema` = '%s'
                """;
        //
        String valueDetail = "";
        // convert fields to json
        ArrayList<String> fields = getListFields(connection, table);
        Gson gson = new Gson();
        if (operationType != 3) {
            valueDetail = "concat('{',";
            //
            int count = 1;
            for (String field : fields) {
                valueDetail += String.format("'\"%s\":','\"',new.%s,'\"'", field, field);
                if (count < fields.size()) {
                    valueDetail += ",',',";
                } else {
                    valueDetail += ",'}')";
                }
                count++;
            }
        } else {
            valueDetail = "''";
        }
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

    public static ArrayList<CDCModel> getCDCs(Connection connection, int offsets, int max_id) {
        ArrayList<CDCModel> listCDCs = new ArrayList<>();
        String getDataQuery = "SELECT * FROM cdc.test_cdc_detail where id > ? and id <= ?";
        try {
            PreparedStatement prpStmt = connection.prepareStatement(getDataQuery);
            prpStmt.setInt(1, offsets);
            prpStmt.setInt(2, max_id);
            ResultSet rs = prpStmt.executeQuery();
            while (rs.next()) {
                listCDCs.add(
                        new CDCModel(rs.getInt("id"), rs.getString("database_url"), rs.getString("database_port"),
                                rs.getString("database_name"), rs.getString("table_name"), rs.getString("schema"),
                                rs.getString("value"), rs.getInt("operation"), rs.getDate("created"))
                );
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        return listCDCs;
    }

    public static void updateOffset(Connection connection, String db, String table , int offsets){
        String query = "update cdc.test_offsets set offsets = ? where `database` = ? and `table` = ? ";
        try {
            PreparedStatement prpStmt = connection.prepareStatement(query);
            prpStmt.setInt(1,offsets);
            prpStmt.setString(2,db);
            prpStmt.setString(3,table);
            prpStmt.executeUpdate();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
    }
}
