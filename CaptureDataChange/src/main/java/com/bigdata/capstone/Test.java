package com.bigdata.capstone;

import utils.db_utils.sqlUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

public class Test {
    public static void main(String[] args) throws SQLException {
        String query = "SELECT * FROM cdc_4912929__cdc.cdc_detail";
        Connection configConnection = sqlUtils.getConnection(sqlUtils.getConnectionString("10.8.0.1", "3306",
                "cdc", "duynt", "Capstone123@"));
        Statement stmt = configConnection.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()){
            System.out.println(rs.getTimestamp("created").getTime() / 1000);
        }
    }
}
