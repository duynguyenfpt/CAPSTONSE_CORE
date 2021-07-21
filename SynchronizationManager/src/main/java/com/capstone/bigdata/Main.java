package com.capstone.bigdata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;

import org.stringtemplate.v4.ST;
import utils.*;

public class Main {
    public static void main(String[] args) throws SQLException {
        String host = args[0];
        String port = args[1];
        String username = args[2];
        String password = args[3];
        String tableName = args[4];
        String dbName = args[5];
        String partitionBy = args[6];
        String jobID = args[7];
        int isAll = Integer.parseInt(args[8]);
        if (isAll == 1) {
            syncAll(host, port, username, password, tableName, dbName, partitionBy, jobID);
        }
    }

    public static void syncAll(String host, String port, String username, String password
            , String tableName, String dbName, String partitionBy, String jobID) throws SQLException {
        // cdc
        System.out.println("table is " + tableName);
        //
        System.out.println("START INGEST CDC");
        String cdcCmd = String.format("java -cp jars/CDC-1.0-SNAPSHOT-jar-with-dependencies.jar " +
                "com.bigdata.capstone.main %s %s %s %s %s %s %s", host, port, dbName, username, password, tableName, jobID);
        System.out.println(cdcCmd);
        runCommand(cdcCmd);
        System.out.println("DONE INGEST");
        // do snapshot
        System.out.println("START SNAPSHOT");
        String cmd = String.format("spark-submit --master yarn --class SparkWriter " +
                "--num-executors 1 --executor-cores 2 --executor-memory 1G " +
                "--jars jars/mysql-connector-java-8.0.25.jar jars/ParquetTest-1.0-SNAPSHOT.jar " +
                "%s %s %s %s %s %s %s %s", dbName, tableName, username, password, host, port, partitionBy, jobID);
        System.out.println(cmd);
//        runCommand(cmd);
        System.out.println("DONE SNAPSHOT");
        System.out.println("UPDATE READINESS");
        Connection connection = sqlUtils.getConnection(
                sqlUtils.getConnectionString("localhost", "3306", "cdc", "duynt", "Capstone123@"));
        sqlUtils.updateReady(host, port, dbName, tableName, connection, 1);
        System.out.println("DONE UPDATING ACTIVENESS");
    }

    public static void runCommand(String cmdToExecute) {
        String osName = System.getProperty("os.name");
        String rawVersion = null;
        StringBuffer outputReport = new StringBuffer();
        if (osName != null && osName.indexOf("Windows") == -1
                && osName.indexOf("SunOS") == -1) {
            Runtime rt = Runtime.getRuntime();
            BufferedReader is = null;
            try {
                // execute the RPM process
                Process proc = rt.exec(new String[]{"sh", "-c", cmdToExecute});

                // read output of the rpm process
                is = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                String tempStr = "";
                while ((tempStr = is.readLine()) != null) {
                    outputReport.append(tempStr.replaceAll(">", "/>\n"));
                    System.out.println(tempStr.replaceAll(">", "/>\n"));
                    tempStr = "";
                }
                int inBuffer;
                while ((inBuffer = is.read()) != -1) {
                    outputReport.append((char) inBuffer);
                }
                // rawVersion = is.readLine();
                // response.append(rawVersion);
                is.close();
                proc.destroy();

            } catch (IOException ioe) {
                System.out.println("Exception executing command " + cmdToExecute + "\n" + ioe);

            } finally {
                try {
                    is.close();
                } catch (final IOException ioe) {
                    System.out.println("Cannot close BufferedStream" + ioe);
                }
            }
        }
    }
}
