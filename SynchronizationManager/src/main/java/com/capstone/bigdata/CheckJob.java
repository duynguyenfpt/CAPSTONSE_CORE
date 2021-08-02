package com.capstone.bigdata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import utils.sqlUtils;

public class CheckJob {
    public static void main(String[] args) {
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    String template = "java -cp jars/SynchronizationManager-1.0.jar:jars/mysql-connector-java-8.0.25.jar:jars/kafka-clients-2.4.1.jar " +
                            "com.capstone.bigdata.Main %s %s %s %s %s %s %s %d %d %d %s";
                    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                    Date date = new Date();
                    System.out.println("START CHECKING REQUEST: " + dateFormat.format(date));
                    System.out.println("NEXT CHECKING IN 10 SECONDS");
                    String query = "" +
                            "SELECT si.server_host,di.port,di.username,di.password,di.database_name," +
                            "tbls.`table_name`,jobs.id as job_id, str.partition_by, " +
                            "str.is_all, str.id as str_id,di.database_type FROM \n" +
                            "(SELECT * FROM webservice_test.jobs\n" +
                            "where number_retries < max_retries and status = 'pending' and deleted = 0) as jobs\n" +
                            "INNER JOIN\n" +
                            "webservice_test.sync_table_requests str\n" +
                            "INNER JOIN\n" +
                            "webservice_test.tables tbls\n" +
                            "INNER JOIN\n" +
                            "webservice_test.database_infos as di\n" +
                            "INNER JOIN\n" +
                            "webservice_test.server_infos as si\n" +
                            "on jobs.request_id = str.request_id\n" +
                            "and str.table_id = tbls.id\n" +
                            "and tbls.database_info_id = di.id \n" +
                            "and si.id = di.server_info_id;";
                    Connection configConnection = sqlUtils.getConnection(sqlUtils.getConnectionString("localhost", "3306",
                            "cdc", "duynt", "Capstone123@"));
                    Statement stmt = configConnection.createStatement();
                    ResultSet rs = stmt.executeQuery(query);
                    while (rs.next()) {
                        String host = rs.getString("server_host");
                        String port = rs.getString("port");
                        String username = rs.getString("username");
                        String password = rs.getString("password");
                        if (Objects.isNull(password) || password.equals("") || password.equals(" ")) {
                            password = "' '";
                        }
                        String database_name = rs.getString("database_name");
                        String table_name = rs.getString("table_name");
                        int job_id = rs.getInt("job_id");
                        int is_all = rs.getInt("is_all");
                        int str_id = rs.getInt("str_id");
                        String partition_by = rs.getString("partition_by");

                        if (Objects.isNull(partition_by) || partition_by.equals("") || partition_by.equals(" ")) {
                            partition_by = "' '";
                        }
                        String database_type = rs.getString("database_type");
                        System.out.println(partition_by);
                        String cmd = String.format(template, host, port, username, password, table_name,
                                database_name, partition_by, str_id, job_id, is_all, database_type);
                        System.out.println(cmd);
                        runCommand(cmd);
                    }
                } catch (Exception sqlException) {
                    sqlException.printStackTrace();
                }
            }
        }, 0, 10, TimeUnit.SECONDS);
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
