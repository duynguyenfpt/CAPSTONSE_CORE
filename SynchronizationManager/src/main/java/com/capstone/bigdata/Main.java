package com.capstone.bigdata;

import com.google.gson.Gson;
import models.MergeRequestModel;
import models.QueryModel;
import models.ReadinessModel;
import utils.sqlUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class Main {
    public static void main(String[] args) throws SQLException {
        String host = args[0];
        String port = args[1];
        String username = args[2];
        String password = args[3];
        if (password.equals(" ")) {
            password = "' '";
        }
        String tableName = args[4];
        String dbName = args[5];
        String partitionBy = args[6];
        if (partitionBy.equals(" ")) {
            partitionBy = "' '";
        }
        int strID = Integer.parseInt(args[7]);
        int jobID = Integer.parseInt(args[8]);
        int isAll = Integer.parseInt(args[9]);
        String database_type = args[10];
        String request_type = args[11];
        String from_date = args[12];
        String to_date = args[13];
        String query = args[14];
        int request_id = Integer.parseInt(args[15]);
        if (request_type.equalsIgnoreCase("ETLRequest")) {
            // etl request
            QueryModel qm = new QueryModel();
            qm.setRequestId(request_id);
            qm.setJobId(jobID);
            qm.setQuery(query);
            qm.setEtlID(strID);
            sqlUtils.requestProducer("localhost:9092", "etl-query", qm);
        } else {
            if (isAll == 0) {
                // sync all request
                syncAll(host, port, username, password, tableName, dbName, partitionBy, jobID, strID, database_type, request_id);
            } else {
                // sync partially
            }
        }
    }

    public static void syncAll(String host, String port, String username, String password
            , String tableName, String dbName, String partitionBy, int jobID, int strID, String database_type, int request_id) throws SQLException {
        // update job status
        Connection configConnection = sqlUtils.getConnection(sqlUtils.getConnectionString("localhost", "3306",
                "cdc", "duynt", "Capstone123@"));
        sqlUtils.updateJobStatus(configConnection, jobID, "processing");
        // cdc
        System.out.println("table is " + tableName);
        //
        System.out.println("START INGEST CDC");
        if (password.equals(" ")) {
            password = "' '";
        }
        String cdcCmd = String.format("java -cp jars/CDC-1.0-SNAPSHOT-jar-with-dependencies.jar " +
                "com.bigdata.capstone.main %s %s %s %s %s %s %s %d %s", host, port, dbName, username, password, tableName, jobID, strID, database_type);
        System.out.println(cdcCmd);
        runCommand(cdcCmd);
        System.out.println("DONE INGEST");
//        // do snapshot
        System.out.println("START SNAPSHOT");
        String cmd = String.format("spark-submit --master yarn --class SparkWriter --num-executors 1 --executor-cores 2 --executor-memory 512M " +
                "--driver-class-path jars/kafka-clients-2.4.1.jar:jars/mysql-connector-java-8.0.25.jar:jars/postgresql-42.2.23.jar:jars/ojdbc8-12.2.0.1.jar " +
                "--jars jars/mysql-connector-java-8.0.25.jar,jars/ojdbc8-12.2.0.1.jar,jars/postgresql-42.2.23.jar jars/ParquetTest-1.0-SNAPSHOT.jar " +
                "%s %s %s %s %s %s %s %d %d %s", dbName, tableName, username, password, host, port, partitionBy, jobID, strID, database_type);
        System.out.println(cmd);
//        runCommand(cmd);
        System.out.println("DONE SNAPSHOT");
        // finish snapshot then make join for merge request
        /*
         * if (request comes from merge request):
         *   send a request to merge
         *
         *
         * */
        PreparedStatement prpStmt = configConnection.prepareStatement("SELECT merge_table_name FROM webservice_test.merge_requests mr\n" +
                "INNER JOIN  webservice_test.request req\n" +
                "on mr.request_type_id = req.id\n" +
                "where request_type_id = ? ");
        prpStmt.setInt(1, request_id);
        ResultSet rs = prpStmt.executeQuery();
        while (rs.next()) {
            MergeRequestModel mrm = new MergeRequestModel();
            mrm.setHost(host);
            mrm.setPort(port);
            mrm.setDatabase(port);
            mrm.setTable(tableName);
            mrm.setDatabaseType(database_type);
            mrm.setMergeTable(rs.getString("merge_table_name"));
            mrm.setUsername(username);
            sqlUtils.mergeRequestProducer("localhost:9092", "merge-request", mrm);
            break;
        }

        try {
            sqlUtils.updateIsProcess(configConnection, strID);
            ArrayList<ReadinessModel> listRemains = sqlUtils.checkRemainRequest(configConnection, jobID);
            System.out.println(listRemains.size());
            int countProcessed = 0;
            for (ReadinessModel rm : listRemains) {
                if (rm.getIsProcess() == 0) {
                    countProcessed++;
                }
            }
            Connection connection = sqlUtils.getConnection(
                    sqlUtils.getConnectionString("localhost", "3306", "cdc", "duynt", "Capstone123@"));
            System.out.println("number remain requests: " + countProcessed);
            if (countProcessed == 0) {
                System.out.println("UPDATE READINESS");
                for (ReadinessModel rm : listRemains) {
                    System.out.println(rm);
                    sqlUtils.updateReady(rm.getHost(), rm.getPort(), rm.getDatabase(), rm.getTable(), connection, 1);
                }
                System.out.println("DONE UPDATING ACTIVENESS");
                sqlUtils.updateJobStatus(configConnection, jobID, "success");
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
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
