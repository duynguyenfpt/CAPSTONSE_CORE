package com.bigdata.capstone;

import models.LogModel;
import org.apache.kafka.common.protocol.types.Field;
import utils.db_utils.sqlUtils;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

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

    public static void main(String[] args) throws SQLException {
        // read from environment
        String host = args[0];
        String port = args[1];
        String db = args[2];
        String username = args[3];
        String password = args[4];
        String table = args[5];
        String jobID = args[6];
        String strID = args[7];
        String databaseType = args[8];
        String connectionString = sqlUtils.getConnectionString(host, port, db, username, password);
        Connection connection = sqlUtils.getConnection(connectionString);
        /*
         * Main pipeline for capture change data
         * */
        Connection configConnection = sqlUtils.getConnection(sqlUtils.getConnectionString("localhost", "3306",
                "cdc", "duynt", "Capstone123@"));
        try {

            LogModel log = new LogModel(Integer.parseInt(jobID), Integer.parseInt(strID),
                    host, port, db, table, 1, "sync_all",
                    5, null, null);
            // 1. init database and table
            try {
                sendLogs(1, "processing", "init database and table", log);
                if (databaseType.equals("mysql")) {
                    initCdcAtSource(host, port, db, username, password);
                } else if (databaseType.equals("postgresql")) {

                }
                sendLogs(1, "success", "done init database and table", log);
            } catch (Exception exception) {
                sendLogs(1, "failed", "failed init database and table", log);
                throw new Exception("failed init database and table");
            }

//            // 2. drop and create three type triggers - create table cdc and meta data
//            try {
//                sendLogs(2, "processing", " drop and create three type triggers - create table cdc and meta data", log);
//                initialize(connection, host, port, db, table);
//                sendLogs(2, "success", "done drop and create three type triggers - create table cdc and meta data", log);
//            } catch (Exception exception) {
//                sendLogs(2, "failed", "failed drop and create three type triggers - create table cdc and meta data", log);
//                throw new Exception("failed drop and create three type triggers - create table cdc and meta data");
//            }
//
//            // 3. init offset for checking
//            try {
//                sendLogs(3, "processing", " init offset for checking", log);
//                sqlUtils.initOffset(configConnection, host, port);
//                sendLogs(3, "success", " done init offset for checking", log);
//            } catch (Exception exception) {
//                sendLogs(3, "failed", " failed init offset for checking", log);
//                throw new Exception("");
//            }
//            // 4. reset monitor
//            try {
//                sendLogs(4, "processing", "reset monitor", log);
//                sqlUtils.resetMonitor(host, port, db, table, configConnection);
//                sendLogs(4, "success", "done reset monitor", log);
//            } catch (Exception exception) {
//                sendLogs(4, "failed", "failed reset monitor", log);
//                throw new Exception("failed reset monitor");
//            }
//            // 5. logging
//            sqlUtils.insertJobLog(jobID, 1, "ingest_cdc", 1);
        } catch (Exception sqlException) {
            sqlException.printStackTrace();
            sqlUtils.insertJobLog(jobID, 1, "ingest_cdc", 0);
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

    public static void initCdcAtSource(String host, String port, String database, String username, String password) throws SQLException {
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

    public static void initCdcAtSourcePostgresql(String host, String port, String database, String username, String password) throws SQLException {
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
        } else {
            list_queries.add(String.format("create database %s", cdcDatabase));
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
                "  created date DEFAULT CURRENT_DATE\n" +
                ");";
        list_queries.add(createTable);
        for (String query : list_queries) {
            Statement statement = connection.createStatement();
            statement.execute(query);
//        }
        }
    }
}
