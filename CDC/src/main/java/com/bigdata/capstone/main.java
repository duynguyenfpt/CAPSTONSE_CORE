package com.bigdata.capstone;

import org.apache.kafka.common.protocol.types.Field;
import utils.db_utils.sqlUtils;

import java.sql.Connection;
import java.sql.SQLException;

public class main {
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
            // 1. drop and create three type triggers - create table cdc and meta data
            initialize(connection, host, port, db, table);
            sqlUtils.initOffset(configConnection, db, host, port);
            sqlUtils.insertJobLog(jobID, 1, "ingest_cdc", 1);
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            sqlUtils.insertJobLog(jobID, 1, "ingest_cdc", 0);
        }
    }

    private static void initialize(Connection connection, String host, String port,
                                   String database, String table) throws SQLException {
        String updateTrigger = sqlUtils.createTrigger(connection, host, port, database, table, 2);
        String insertTrigger = sqlUtils.createTrigger(connection, host, port, database, table, 1);
        String deleteTrigger = sqlUtils.createTrigger(connection, host, port, database, table, 3);
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
}
