package com.bigdata.capstone;

import utils.SqlUtils;
import utils.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

public class CheckSchema {

    private static void checkSchema(String host, String databaseType, String databaseName, String key, Connection schemaConnection) throws Exception {
        System.out.println(String.format("CHECKING DATABASE %s ON HOST %s", databaseName, host));
        Connection connection = SqlUtils.getConnectionFromHostAndDB(host, databaseType, key, schemaConnection);
        if (connection != null) {
            List<String> listTables = SqlUtils.listTable(connection, databaseName);
            List<String> listTableFromDBSchema = SqlUtils.getListTable(databaseType, schemaConnection);
            List<List<String>> compareTables = Utils.compareDiff(listTables, listTableFromDBSchema);
            System.out.println("number common tables: " + compareTables.get(0).size());
            System.out.println("number different tables: " + compareTables.get(1).size());
            SqlUtils.bashInsertListTableSchema(connection, compareTables.get(1), databaseType, schemaConnection);
            SqlUtils.captureSchemaChange(connection, compareTables.get(0), databaseType, schemaConnection);
        } else {
            throw new Exception("THERE IS NO SUCH A DATABASE, PLEASE ENTER INFORMATION TO IMPORT DATA");
        }
    }

    public static void checkAll(String schemaConnection, String key) throws Exception {
        Connection connection = SqlUtils.getConnection(schemaConnection);
        String getAllDBQuery = "SELECT * FROM database_connect_detail WHERE is_success = 1";
        PreparedStatement prpStmt = connection.prepareStatement(getAllDBQuery);
        ResultSet rs = prpStmt.executeQuery();
        while (rs.next()) {
            String host = rs.getString("db_host");
            String dbType = rs.getString("db_type");
            String dbName = rs.getString("db_name");
            if (!dbType.equals("das")) {
                checkSchema(host, dbType, dbName, key, connection);
            }
        }
    }

    public static void main(String[] args) {
        String schemaDetectorConnection = args[0];
        //
        try {
            String key = Utils.getKeyFromPath(args[1]);
            checkAll(schemaDetectorConnection, key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //
    }
}
