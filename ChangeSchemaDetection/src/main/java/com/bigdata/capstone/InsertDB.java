package com.bigdata.capstone;

import utils.SqlUtils;
import utils.Utils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import models.AES;


public class InsertDB {

    public static void execute(String host, String port, String username, String password, String db, String db_name, String key, String schemaConnection) throws Exception {
        Connection connectionToSchema = SqlUtils.getConnection(schemaConnection);
        String dbType = SqlUtils.checkExistTable(host, db, connectionToSchema);
        if (dbType == null) {
            // if not exits then insert straight to database without decrypting
            String connectionString = SqlUtils.getConnectionString(host, port, db, username, password);
            Connection connection = SqlUtils.getConnection(connectionString);
            List<String> listTables = SqlUtils.listTable(connection, db);
            // if not exist then insert all table of database
            if (db_name == null) {
                throw new Exception("DATABASE TYPE SPECIFIC MUST BE INPUTTED");
            }
            // INSERT TO DATABASE CONNECTION TABLE
            SqlUtils.insertDBType(host, port, username, AES.encrypt(password, key), db, db_name, connectionToSchema);
            //
            SqlUtils.bashInsertListTableSchema(connection, listTables, db_name, connectionToSchema);
        } else {
            //
            String connectionString = SqlUtils.getConnectionString(host, port, db, username, AES.decrypt(password, key));
            System.out.println("after decrypt: " + connectionString);
            Connection connection = SqlUtils.getConnection(connectionString);
            List<String> listTables = SqlUtils.listTable(connection, db);
            // if exist in database connection
            List<String> listTableFromDBSchema = SqlUtils.getListTable(dbType, connectionToSchema);
            List<List<String>> compareTables = Utils.compareDiff(listTables, listTableFromDBSchema);
            System.out.println("number common tables: " + compareTables.get(0).size());
            System.out.println("number different tables: " + compareTables.get(1).size());

            /*
             * compareTables[0] : is common tables of database and previous version
             * compareTables[1] : is different tables of database and previous version
             * */
            // add new tables schema
            SqlUtils.bashInsertListTableSchema(connection, compareTables.get(1), dbType, connectionToSchema);
            // compare schema of common tables
            SqlUtils.captureSchemaChange(connection, compareTables.get(0), dbType, connectionToSchema);
//        }
        }
    }

    public static void updateStatus(String host, String dbType, boolean is_success, String schemaConnection) throws IOException, SQLException {
        Connection connection = SqlUtils.getConnection(schemaConnection);
        if (is_success) {
            String query = "UPDATE database_connect_detail set is_success = 1 where db_type = ? and db_host = ?";
            PreparedStatement prStmt = connection.prepareStatement(query);
            prStmt.setString(1, dbType);
            prStmt.setString(2, host);
            prStmt.executeUpdate();
        } else {
            String query = "UPDATE database_connect_detail set number_retries = number_retries + 1 where db_type = ? and db_host = ?";
            PreparedStatement prStmt = connection.prepareStatement(query);
            prStmt.setString(1, dbType);
            prStmt.setString(2, host);
            prStmt.executeUpdate();
        }
    }


    public static void main(String[] args) {
        String schemaDetectorConnection = args[1];
        System.out.println(schemaDetectorConnection);
        String host = args[2];
        String port = args[3];
        String user = args[4];
        String password = args[5];
        String db = args[6];
        // default if not specify
        String db_name = db;
        if (args.length >= 8) {
            db_name = args[7];
        }
        try {
            //String key = Utils.getKeyFromPath(args[0]);
            String key = "k1xG7kmMG41GVQSRn2xL95Yrm6O5wvSG";
            execute(host, port, user, password, db, db_name, key, schemaDetectorConnection);
            updateStatus(host, db_name, true, schemaDetectorConnection);
        } catch (Exception e) {
            try {
                updateStatus(host, db_name, false, schemaDetectorConnection);
            } catch (IOException ioException) {
                ioException.printStackTrace();
            } catch (SQLException exception) {
                exception.printStackTrace();
            }
            e.printStackTrace();
        }

    }
}
