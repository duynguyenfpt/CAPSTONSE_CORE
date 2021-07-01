package com.bigdata.capstone;
import utils.db_utils.sqlUtils;

import java.sql.Connection;
import java.sql.SQLException;

public class main {
    public static void main(String[] args) {
        // read from enviroment
        String host = args[0];
        String port = args[1];
        String db = args[2];
        String username = args[3];
        String password = args[4];
        String table = args[5];
        String connectionString = sqlUtils.getConnectionString(host,port,db,username,password);
        Connection connection = sqlUtils.getConnection(connectionString);
        /*
         * Main pipeline for capture change data
         * */

        //
        // 1. drop and create three type triggers - create table cdc and meta data
        initialize(connection,host,port,db,table);
        // 2. check change service
        // 3. pull data  and publish it to kafka
        // 4. update offset
    }

    private static void initialize(Connection connection, String host, String port,
                                   String database,String table){
        String updateTrigger = sqlUtils.createTrigger(connection,host,port,database,table,2);
        String insertTrigger = sqlUtils.createTrigger(connection,host,port,database,table,1);
        String deleteTrigger = sqlUtils.createTrigger(connection,host,port,database,table,3);
        //
        try {
            connection.createStatement().execute(String.format("DROP TRIGGER IF EXISTS test_after_%s_%s_%s",database,table,"INSERT"));
            connection.createStatement().execute(String.format("DROP TRIGGER IF EXISTS test_after_%s_%s_%s",database,table,"DELETE"));
            connection.createStatement().execute(String.format("DROP TRIGGER IF EXISTS test_after_%s_%s_%s",database,table,"UPDATE"));
            connection.createStatement().execute(updateTrigger);
            connection.createStatement().execute(insertTrigger);
            connection.createStatement().execute(deleteTrigger);
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        //
    }
}
