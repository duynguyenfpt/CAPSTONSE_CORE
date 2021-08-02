package com.capstone.bigdata;

import utils.sqlUtils;

import java.sql.Connection;

public class Test {
    public static void main(String[] args) {
        try {
            Connection configConnection = sqlUtils.getConnection(sqlUtils.getConnectionString("localhost", "3306",
                    "cdc", "duynt", "Capstone123@"));
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
