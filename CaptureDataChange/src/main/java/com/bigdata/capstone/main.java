package com.bigdata.capstone;

import models.LogModel;
import org.apache.kafka.common.protocol.types.Field;
import utils.db_utils.sqlUtils;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class main {
    public static final String prefix = "cdc_4912929_";

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

//    public static void main(String[] args) throws SQLException {
//        String host = "127.0.0.1";
//        String port = "1521";
//        String db = "";
//        String username = "system";
//        String password = "capstone";
//        String table = "customer";
//        String jobID = args[6];
//        String strID = args[7];
//        String databaseType = args[8];
//        Connection configConnection = sqlUtils.getConnection(sqlUtils.getConnectionString("localhost", "3307",
//                "cdc", "duynt", "Capstone123@"));
//        String sid = sqlUtils.getSID(host, port, configConnection);
//        System.out.println(sid);
//        Connection connection = sqlUtils.getConnectionOracle(username, password, host, port, sid);
//        initializeOracle(connection, host, port, db, table, "system");
//    }

    public static void main(String[] args) throws SQLException {
        // read from environment
        String host = args[0];
        String port = args[1];
        String db = args[2];
        String username = args[3];
        String password = args[4];
        System.out.println(password);
        String table = args[5];
        String jobID = args[6];
        String strID = args[7];
        String databaseType = args[8];
        String connectionString = sqlUtils.getConnectionString(host, port, db, username, password, databaseType);
        Connection configConnection = sqlUtils.getConnection(sqlUtils.getConnectionString("localhost", "3306",
                "cdc", "duynt", "Capstone123@"));
        //
        Connection connection = null;
        String sid = "";
        if (databaseType.equals("oracle")) {
            sid = sqlUtils.getSID(host, port, configConnection);
            System.out.println("sid is: " + sid);
            connection = sqlUtils.getConnectionOracle(username, password, host, port, sid);
        } else {
            connection = sqlUtils.getConnection(connectionString);
        }
        //
        /*
         * Main pipeline for capture change data
         * */
        try {

            LogModel log = new LogModel(Integer.parseInt(jobID), Integer.parseInt(strID),
                    host, port, db, table, 1, "sync_all",
                    5, null, null);
            // 1. init database and table
            String owner = "";
            try {
                sendLogs(1, "processing", "init database and table", log);
                if (databaseType.equals("mysql")) {
                    initCdcAtSource(host, port, db, username, password);
                } else if (databaseType.equals("postgresql")) {
                    initCdcAtSourcePostgresql(host, port, db, username, password);
                } else if (databaseType.equals("oracle")) {
                    owner = initCdcAtSourceOracle(host, port, username, password, sid);
                    System.out.println("owner is: " + owner);
                } else {
                    sendLogs(1, "failed", "table not supported!", log);
                    throw new Exception("table not supported!");
                }
                sendLogs(1, "success", "done init database and table", log);
            } catch (Exception exception) {
                exception.printStackTrace();
                sendLogs(1, "failed", "failed init database and table", log);
                throw new Exception("failed init database and table");
            }

            // 2. drop and create three type triggers - create table cdc and meta data
            try {
                sendLogs(2, "processing", " drop and create three type triggers - create table cdc and meta data", log);
                if (databaseType.equals("mysql")) {
                    initialize(connection, host, port, db, table);
                } else if (databaseType.equals("postgresql")) {
                    initializePostgresql(connection, host, port, db, table, username, password);
                } else if (databaseType.equals("oracle")) {
                    initializeOracle(connection, host, port, db, table, owner);
                }
                sendLogs(2, "success", "done drop and create three type triggers - create table cdc and meta data", log);
            } catch (Exception exception) {
                sendLogs(2, "failed", "failed drop and create three type triggers - create table cdc and meta data", log);
                throw new Exception("failed drop and create three type triggers - create table cdc and meta data");
            }

//
            // 3. init offset for checking
            try {
                sendLogs(3, "processing", " init offset for checking", log);
                sqlUtils.initOffset(configConnection, host, port, databaseType);
                sendLogs(3, "success", " done init offset for checking", log);
            } catch (Exception exception) {
                sendLogs(3, "failed", " failed init offset for checking", log);
                throw new Exception("");
            }
            // 4. reset monitor
            sqlUtils.resetMonitor(host, port, db, table, configConnection);
            try {
                sendLogs(4, "processing", "reset monitor", log);
                sqlUtils.resetMonitor(host, port, db, table, configConnection);
                sendLogs(4, "success", "done reset monitor", log);
            } catch (Exception exception) {
                sendLogs(4, "failed", "failed reset monitor", log);
                throw new Exception("failed reset monitor");
            }
//            // 5. logging
//            sqlUtils.insertJobLog(jobID, 1, "ingest_cdc", 1);
        } catch (Exception sqlException) {
            sqlException.printStackTrace();
        } finally {
            if (!Objects.isNull(configConnection)) {
                configConnection.close();
            }
            if (!Objects.isNull(connection)) {
                configConnection.close();
            }
        }
    }

    private static void initialize(Connection connection, String host, String port,
                                   String database, String table) throws SQLException {
        String cdcDB = String.format("%s_cdc", prefix);
        String cdcTable = "cdc_detail";
        String updateTrigger = sqlUtils.createTrigger(connection, host, port, database, table, 2, cdcDB, cdcTable);
        String insertTrigger = sqlUtils.createTrigger(connection, host, port, database, table, 1, cdcDB, cdcTable);
        String deleteTrigger = sqlUtils.createTrigger(connection, host, port, database, table, 3, cdcDB, cdcTable);
        //
        connection.createStatement().execute(String.format("DROP TRIGGER IF EXISTS test_after_%s_%s_%s", database, table, "INSERT"));
        System.out.println("Finish creating insert trigger !!");
        connection.createStatement().execute(String.format("DROP TRIGGER IF EXISTS test_after_%s_%s_%s", database, table, "DELETE"));
        System.out.println("Finish creating delete trigger !!");
        connection.createStatement().execute(String.format("DROP TRIGGER IF EXISTS test_after_%s_%s_%s", database, table, "UPDATE"));
        System.out.println("Finish creating updates trigger !!");
        connection.createStatement().execute(updateTrigger);
        connection.createStatement().execute(insertTrigger);
        connection.createStatement().execute(deleteTrigger);
        //
    }

    private static void initializePostgresql(Connection connection, String host, String port,
                                             String database, String table, String username, String password) throws SQLException {
        String cdcDB = String.format("%s_cdc", prefix);
        String cdcTable = "cdc_detail";
        String updateTriggerFunction = sqlUtils.createTriggerFunctionPostgresql(connection, host, port, database, table, 2, cdcDB, cdcTable, username, password);
        String insertTriggerFunction = sqlUtils.createTriggerFunctionPostgresql(connection, host, port, database, table, 1, cdcDB, cdcTable, username, password);
        String deleteTriggerFunction = sqlUtils.createTriggerFunctionPostgresql(connection, host, port, database, table, 3, cdcDB, cdcTable, username, password);
        //
        connection.createStatement().execute(String.format("DROP TRIGGER IF EXISTS \"%s.%s_%s\" ON  %s;", database, table, "INSERT", table));
        System.out.println("Finish creating insert trigger !!");
        connection.createStatement().execute(String.format("DROP TRIGGER IF EXISTS \"%s.%s_%s\" ON  %s;", database, table, "DELETE", table));
        System.out.println("Finish creating delete trigger !!");
        connection.createStatement().execute(String.format("DROP TRIGGER IF EXISTS \"%s.%s_%s\" ON  %s;", database, table, "UPDATE", table));
        //
        String triggerTemplate = "" +
                "CREATE TRIGGER \"%s.%s_%s\" \n" +
                "  AFTER %s \n" +
                "  ON %s\n" +
                "  FOR EACH ROW\n" +
                "  EXECUTE PROCEDURE \"capture_%s_%s\"()";
        //
//        System.out.println("Finish creating updates trigger !!");
        connection.createStatement().execute(updateTriggerFunction);
        connection.createStatement().execute(insertTriggerFunction);
        connection.createStatement().execute(deleteTriggerFunction);
        //
        connection.createStatement().execute(String.format(triggerTemplate, database, table, "INSERT", "INSERT", table, "INSERT", table));
        connection.createStatement().execute(String.format(triggerTemplate, database, table, "UPDATE", "UPDATE", table, "UPDATE", table));
        connection.createStatement().execute(String.format(triggerTemplate, database, table, "DELETE", "DELETE", table, "DELETE", table));
        connection.close();
    }

    private static void initializeOracle(Connection connection, String host, String port,
                                         String database, String table, String owner) throws SQLException {
        String cdcDB = String.format("%s_cdc", prefix);
        String cdcTable = "cdc_detail";
        String updateTriggerFunction = sqlUtils.createTriggerOracle(connection, host, port, database, table, 2, cdcDB, owner);
        String insertTriggerFunction = sqlUtils.createTriggerOracle(connection, host, port, database, table, 1, cdcDB, owner);
        String deleteTriggerFunction = sqlUtils.createTriggerOracle(connection, host, port, database, table, 3, cdcDB, owner);
        //
//        System.out.println(updateTriggerFunction);
//        System.out.println("=========================");
//        System.out.println(insertTriggerFunction);
//        System.out.println("=========================");
//        System.out.println(deleteTriggerFunction);
        //
        connection.createStatement().execute(updateTriggerFunction);
        connection.createStatement().execute(insertTriggerFunction);
        connection.createStatement().execute(deleteTriggerFunction);
    }


    public static void initCdcAtSource(String host, String port, String database, String username, String password) throws
            SQLException {
        String dropDatabase = String.format("drop schema if exists %s_cdc", prefix);
        String createDatabase = String.format("create database if not exists %s_cdc", prefix);
        String useDatabase = String.format("use %s_cdc", prefix);
        String createTable = "" +
                "CREATE TABLE IF NOT EXISTS `cdc_detail` (\n" +
                "  `id` int NOT NULL AUTO_INCREMENT,\n" +
                "  `database_url` varchar(45) DEFAULT NULL,\n" +
                "  `database_port` varchar(6) DEFAULT NULL,\n" +
                "  `database_name` varchar(20) DEFAULT NULL,\n" +
                "  `table_name` varchar(20) DEFAULT NULL,\n" +
                "  `schema` varchar(200) DEFAULT NULL,\n" +
                "  `value` varchar(200) DEFAULT NULL,\n" +
                "  `operation` int DEFAULT NULL,\n" +
                "  `created` datetime DEFAULT CURRENT_TIMESTAMP,\n" +
                "  PRIMARY KEY (`id`)\n" +
                ");";
        ArrayList<String> list_queries = new ArrayList<String>(Arrays.asList(createDatabase, useDatabase, createTable));
        Connection connection = sqlUtils.getConnection(sqlUtils.getConnectionString(host, port, database, username, password));
        //
        String[] message = new String[]{
//                "DELETE SUCCESSFULLY!",
                "CREATE DATABASE SUCCESSFULLY"
                , "USE DATABASE", "CREATED TABLE"};
        //
        int index = 0;
        for (String query : list_queries) {
            Statement statement = connection.createStatement();
            statement.execute(query);
            System.out.println(message[index]);
            index++;
        }
    }

    public static void initCdcAtSourcePostgresql(String host, String port, String database, String username, String
            password) throws SQLException {
        String cdcDatabase = String.format("%s_cdc", prefix);
        // check if exist database
        Connection connection = sqlUtils.getConnection(sqlUtils.getConnectionString(host, port, database, username, password, "postgresql"));
        //
        int count = 0;
        String queryCheckDB = "SELECT datname FROM pg_catalog.pg_database WHERE datname=?";
        PreparedStatement prpSmt = connection.prepareStatement(queryCheckDB);
        prpSmt.setString(1, cdcDatabase);
        ResultSet rs = prpSmt.executeQuery();
        while (rs.next()) {
            count++;
        }
        ArrayList<String> list_queries = new ArrayList<String>();
        if (count > 0) {
            // if exist database
            // ignore
            connection.close();
            return;
        } else {
            Statement statement = connection.createStatement();
            statement.execute(String.format("create database %s", cdcDatabase));
        }
        connection.close();
        // switch to cdc database
        connection = sqlUtils.getConnection(sqlUtils.getConnectionString(host, port, cdcDatabase, username, password, "postgresql"));
        String createTable = "" +
                "CREATE TABLE IF NOT EXISTS public.cdc_detail (\n" +
                "  id SERIAL,\n" +
                "  database_url varchar(45) DEFAULT NULL,\n" +
                "  database_port varchar(6) DEFAULT NULL,\n" +
                "  database_name varchar(20) DEFAULT NULL,\n" +
                "  table_name varchar(20) DEFAULT NULL,\n" +
                "  schema varchar(200) DEFAULT NULL,\n" +
                "  value varchar(200) DEFAULT NULL,\n" +
                "  operation int DEFAULT NULL,\n" +
                "  created TIMESTAMP DEFAULT CURRENT_TIMESTAMP \n" +
                ");";
        Statement statement = connection.createStatement();
        statement.execute(createTable);
        connection.close();
    }

    public static String initCdcAtSourceOracle(String host, String port, String username, String
            password, String sid) throws SQLException {
        String cdcDatabase = String.format("%s_cdc", prefix);
        // check if exist database
        Connection connection = sqlUtils.getConnectionOracle(username, password, host, port, sid);
        //
        int count = 0;
        String queryCheckDB = "SELECT\n" +
                "  table_name, owner\n" +
                "FROM\n" +
                "  all_tables\n" +
                "WHERE upper(table_name)='CDC_4912929__CDC'\n" +
                "ORDER BY\n" +
                "  owner, table_name";
        PreparedStatement prpSmt = connection.prepareStatement(queryCheckDB);
        ResultSet rs = prpSmt.executeQuery();
        String owner = "";
        while (rs.next()) {
            owner = rs.getString("owner");
        }
        ArrayList<String> list_queries = new ArrayList<String>();
        if (!owner.equals("")) {
            // if exist database
            // ignore
            connection.close();
            return owner;
        }
        String createTable = "" +
                "CREATE TABLE %s (\n" +
                "  id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1),\n" +
                "  database_url varchar(45) DEFAULT NULL,\n" +
                "  database_port varchar(6) DEFAULT NULL,\n" +
                "  database_name varchar(20) DEFAULT NULL,\n" +
                "  table_name varchar(20) DEFAULT NULL,\n" +
                "  schema varchar(200) DEFAULT NULL,\n" +
                "  value varchar(200) DEFAULT NULL,\n" +
                "  operation int DEFAULT NULL,\n" +
                "  created timestamp DEFAULT current_timestamp\n" +
                ")";
        Statement statement = connection.createStatement();
        statement.execute(String.format(createTable, cdcDatabase));
        // grant public
        String sql = "GRANT SELECT,INSERT ON %s to PUBLIC\n";
        Statement publicStmt = connection.createStatement();
        publicStmt.execute(String.format(sql, cdcDatabase));
        connection.close();
        return username;
    }
}
