package utils;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import models.AES;
import models.Alert.AlertModel;
import models.ColumnProperty;
import models.DDLModel;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlUtils {

    public static String getConnectionString(String host, String port, String db, String userName, String password) {
//        return String.format("jdbc:mysql://%s:%s/%s?user=%s&password=%s&useSSL=false&characterEncoding=utf-8&verifyServerCertificate=false&autoReconnect=true", host, port, db, userName, password);
        return String.format("jdbc:mysql://%s:%s/%s?user=%s&password=%s&useSSL=false&characterEncoding=utf-8", host, port, db, userName, password);
    }

    public static Connection getConnection(String dbURL, String userName, String password) {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(dbURL, userName, password);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return conn;
    }

    public static Connection getConnection(String dbURL, String resource) throws IOException {
        Properties properties = new Properties();
        InputStream is = SqlUtils.class.getClassLoader().getResourceAsStream(resource);
        properties.load(is);
        String databaseURL = properties.getProperty(dbURL);
        return getConnection(databaseURL);
    }

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

    public static Connection getConnectionFromHostAndDB(String host, String database, String key, Connection connection) throws IOException, SQLException {
        String getDBInfoQuery = "SELECT * FROM database_connect_detail WHERE db_host = ? and db_type = ? ";
        PreparedStatement getInfoStmt = connection.prepareStatement(getDBInfoQuery);
        getInfoStmt.setString(1, host);
        getInfoStmt.setString(2, database);
        ResultSet rs = getInfoStmt.executeQuery();
        while (rs.next()) {
            String port = rs.getString("db_port");
            String username = rs.getString("db_username");
            String password = rs.getString("db_password");
            String db = rs.getString("db_name");
            return SqlUtils.getConnection(SqlUtils.getConnectionString(host, port, db, username, AES.decrypt(password, key)));
        }
        return null;
    }

    public static List<String> listTable(Connection connection, String db) throws SQLException {
        ArrayList<String> listTables = new ArrayList<>();
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("show tables");
        while (rs.next()) {
            listTables.add(rs.getString(String.format("Tables_in_%s", db)));
        }
        return listTables;
    }

    public static List<String> getListTable(String db, Connection connection) throws SQLException, IOException {
        ArrayList<String> listTables = new ArrayList<>();
        String searchQuery = "select distinct table_name from table_schema_detail where db_type = ?";
        PreparedStatement stmt = connection.prepareStatement(searchQuery);
        stmt.setString(1, db);
        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
            listTables.add(rs.getString(String.format("table_name", db)));
        }
        return listTables;
    }

    public static void insertDBType(String host, String port,
                                    String username, String password, String dbName, String dbType, Connection connection) throws SQLException, IOException {
        String insertQuery = "insert into database_connect_detail(db_type,db_host,db_port,db_name,db_username,db_password) values(?,?,?,?,?,?)";
        PreparedStatement prpStmt = connection.prepareStatement(insertQuery);
        prpStmt.setString(1, dbType);
        prpStmt.setString(2, host);
        prpStmt.setString(3, port);
        prpStmt.setString(4, dbName);
        prpStmt.setString(5, username);
        prpStmt.setString(6, password);
        prpStmt.executeUpdate();
    }

    public static boolean compareSchema(String dbType, String table, String oldSchema, String newSchema, ArrayList<DDLModel> result) {
        Gson gson = new Gson();
        List<ColumnProperty> listOld = gson.fromJson(oldSchema, new TypeToken<List<ColumnProperty>>() {
        }.getType());
        List<ColumnProperty> listNew = gson.fromJson(newSchema, new TypeToken<List<ColumnProperty>>() {
        }.getType());
        List<DDLModel> difference = Utils.compareDiffColumn(listOld, listNew, table, dbType);
        if (difference.size() > 0) {
            result.addAll(difference);
            return false;
        }
        return true;
    }

    public static String getSchemaFromDB(Connection connection, String dbType, String table) throws SQLException {
        String getLatestSchemaVersion = "SELECT table_schema from table_schema_detail as tmp\n" +
                "inner join\n" +
                "(SELECT max(id)  as max_id, db_type,table_name FROM table_schema_detail\n" +
                "group by db_type, table_name) as tmp2\n" +
                "on tmp.db_type = tmp2.db_type and tmp.table_name = tmp2.table_name and tmp.id = tmp2.max_id\n" +
                "where tmp.db_type = ? and tmp.table_name = ?";
        PreparedStatement prpStmt = connection.prepareStatement(getLatestSchemaVersion);
        prpStmt.setString(1, dbType);
        prpStmt.setString(2, table);
        ResultSet rs = prpStmt.executeQuery();
        while (rs.next()) {
            return rs.getString("table_schema");
        }
        return null;
    }

    public static void captureSchemaChange(Connection connection, List<String> listTable, String dbType, Connection dasConnection) throws SQLException, IOException {
        String insertQuery = "insert into table_schema_detail(db_type,table_name,table_schema) values (?,?,?)";
        PreparedStatement insertStmt = dasConnection.prepareStatement(insertQuery);
        ArrayList<DDLModel> changesCapture = new ArrayList<>();
        for (String table : listTable) {
            String newSchema = getTableSchema(connection, table);
            String oldSchema = getSchemaFromDB(dasConnection, dbType, table);
            if (!compareSchema(dbType, table, oldSchema, newSchema, changesCapture)) {
                insertStmt.setString(1, dbType);
                insertStmt.setString(2, table);
                insertStmt.setString(3, newSchema);
                insertStmt.addBatch();
            }
        }
        insertStmt.executeBatch();
        if (changesCapture.size() > 0) {
            String insertChangeQuery = "insert into ddl_detail(db_type,db_table,change_field,type_change,old_value,new_value) values(?,?,?,?,?,?)";
            PreparedStatement insertChangeStmt = dasConnection.prepareStatement(insertChangeQuery);
            int countCheck = 0;
            for (DDLModel change : changesCapture) {
                countCheck++;
                insertChangeStmt.setString(1, change.getDbType());
                insertChangeStmt.setString(2, change.getDbTable());
                insertChangeStmt.setString(3, change.getFieldChanged());
                insertChangeStmt.setString(4, change.getChangeType());
                insertChangeStmt.setString(5, change.getOldValue());
                insertChangeStmt.setString(6, change.getNewValue());
                insertChangeStmt.addBatch();
                if (countCheck % 1000 == 0 || countCheck == changesCapture.size()) {
                    insertChangeStmt.executeBatch();
                }
                // ALERT CHANGE TYPE
                String changeType = "CHANGED";
                if (change.getOldValue() == null) {
                    changeType = "ADDED";
                } else if (change.getNewValue() == null) {
                    changeType = "DELETED";
                }
                ;
                // send alert
                AlertModel alert = new AlertModel();
                alert.setMsg(String.format("TABLE %s@%s - FIELD: %s IS %s", change.getDbType(), Utils.getHostFromConnection(connection), change.getFieldChanged(), changeType));
                alert.setMetrics(change);
                alert.setEntity_name(String.format("%s.%s.%s@%s", change.getDbType(), change.getDbTable(), change.getFieldChanged(), Utils.getHostFromConnection(connection)));
                alert.setEntity_type("COLUMN");
                System.out.println("DEBUG: " + SendAlertUtils.sendPOST("http://ghtk-datanode02:9876/api/alert/", alert));
            }
        }
    }

    public static void bashInsertListTableSchema(Connection connection, List<String> listTables, String dbType, Connection connectionToDas) throws SQLException, IOException {
        String sql = "insert into table_schema_detail(db_type,table_name,table_schema) values (?,?,?)";
        PreparedStatement prpStmt = connectionToDas.prepareStatement(sql);
        int index = 0;
        for (String tableName : listTables) {
            String schema = getTableSchema(connection, tableName);
            if (schema != null) {
                prpStmt.setString(1, dbType);
                prpStmt.setString(2, tableName);
                prpStmt.setString(3, schema);
                prpStmt.addBatch();
                index++;
                // bash 1000 query each time
                if (index % 1000 == 0 || index == listTables.size()) {
                    prpStmt.executeBatch();
                }
            }
        }
    }


    public static String checkExistTable(String dbHost, String dbName, Connection connection) throws SQLException, IOException {
        //
        String query = "SELECT * FROM database_connect_detail " +
                "where db_host = ? and db_name = ? ";
        PreparedStatement preparedStmt = connection.prepareStatement(query);
        preparedStmt.setString(1, dbHost);
        preparedStmt.setString(2, dbName);
        ResultSet rs = preparedStmt.executeQuery();
        while (rs.next()) {
            return rs.getString("db_type");
        }
        return null;
    }

    public static String getTableSchema(Connection connection, String tableName) throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs;
        try {
            rs = stmt.executeQuery(String.format("DESCRIBE `%s`", tableName));
        } catch (Exception exception) {
            return null;
        }
        ArrayList<ColumnProperty> listCol = new ArrayList<>();
        while (rs.next()) {
            ColumnProperty cp = new ColumnProperty();
            String field = rs.getString("field");
            cp.setField(field);
            String type = rs.getString("type");
            if (type.contains("(")) {
                /*
                 * if data type looks like varchar(523)
                 * then extract 523
                 * */
                Pattern pattern = Pattern.compile("\\d+");
                Matcher matcher = pattern.matcher(type);
                if (matcher.find()) {
                    cp.setTypeSize(Integer.parseInt(matcher.group(0)));
                } else {
                    cp.setTypeSize(null);
                }
                cp.setType(type.substring(0, type.indexOf("(")));
            } else {
                cp.setType(type);
            }

            if (type.endsWith(" signed")) {
                /*
                 * if data type looks like int(11) unsigned or int(10) signed
                 * then extract unsigned or signed
                 * */
                cp.setSigned(true);
            } else if (type.endsWith(" unsigned")) {
                cp.setSigned(false);
            } else {
                cp.setSigned(null);
            }
            ArrayList<String> possibleValues = null;
            if (type.startsWith("enum")) {
                Pattern pattern = Pattern.compile("'((\\w)+)'");
                Matcher matcher = pattern.matcher(type);
                possibleValues = new ArrayList<>();
                while (matcher.find()) {
                    possibleValues.add(matcher.group(1));
                }
                cp.setPossibleValues(possibleValues);
            }
            cp.setDefaultValue(rs.getString("default"));
            listCol.add(cp);
        }
        rs.close();
        stmt.close();
        // sort according to field
        Collections.sort(listCol);
        Gson gson = new Gson();
        return gson.toJson(listCol);
    }
}
