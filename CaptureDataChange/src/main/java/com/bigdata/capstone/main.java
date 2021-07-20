package com.bigdata.capstone;

import org.apache.kafka.common.protocol.types.Field;
import utils.db_utils.sqlUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class main {
    public static final String prefix = "cdc_4912929_";

    public static void main(String[] args) throws SQLException {
        // read from environment
        String host = args[0];
        String port = args[1];
        String db = args[2];
        String username = args[3];
        String password = args[4];
        String table = args[5];
        String jobID = args[6];
        String connectionString = sqlUtils.getConnectionString(host, port, db, username, password);
        Connection connection = sqlUtils.getConnection(connectionString);
        /*
         * Main pipeline for capture change data
         * */
        Connection configConnection = sqlUtils.getConnection(sqlUtils.getConnectionString("localhost", "3306",
                "cdc", "duynt", "Capstone123@"));
        try {
            // 1. init database and table
            initCdcAtSource(host, port, db, username, password);
            // 2. drop and create three type triggers - create table cdc and meta data
            initialize(connection, host, port, db, table);
            // 3. init offset for checking
            sqlUtils.initOffset(configConnection, db, host, port);
            // 4. reset monitor
            sqlUtils.resetMonitor(host, port, db, table, configConnection);
            // 5. logging
            sqlUtils.insertJobLog(jobID, 1, "ingest_cdc", 1);
        } catch (SQLException sqlException) {
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
                ") ENGINE=InnoDB AUTO_INCREMENT=36 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;";
        ArrayList<String> list_queries = new ArrayList<String>(Arrays.asList(createDatabase, useDatabase, createTable));
        Connection connection = sqlUtils.getConnection(sqlUtils.getConnectionString(host, port, database, username, password));
        //
        String[] message = new String[]{"DELETE SUCCESSFULLY!", "CREATE DATABASE SUCCESSFULLY"
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
}
