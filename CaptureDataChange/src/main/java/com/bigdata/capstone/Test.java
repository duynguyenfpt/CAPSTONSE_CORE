package com.bigdata.capstone;

import utils.db_utils.sqlUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

public class Test {
    public static void main(String[] args) throws SQLException {
//        Connection connection = sqlUtils.getConnection(
//                sqlUtils.getConnectionString("10.8.0.1","3306","test","duynt","Capstone123@"));
//        //
//        Statement statement= connection.createStatement();
//        statement.execute("create database test_create");
//        //
        try {
            Connection configConnection = sqlUtils.getConnection(sqlUtils.getConnectionString("localhost", "3306",
                    "cdc", "duynt", "Capstone123@"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
