package com.capstone.bigdata;

import utils.sqlUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CheckJob {
    public static void main(String[] args) {
        ConnectionSingleton cs = ConnectionSingleton.getInstance();
        Connection configConnection = cs.connection;
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    String template = "java -cp jars/SynchronizationManager-1.0-jar-with-dependencies.jar:jars/mysql-connector-java-8.0.25.jar:jars/kafka-clients-2.4.1.jar " +
                            "com.capstone.bigdata.Main %s %s %s %s %s %s %s %d %d %d %s %s %s %s %s %s";
                    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                    Date date = new Date();
                    System.out.println("START CHECKING REQUEST: " + dateFormat.format(date));
                    System.out.println("NEXT CHECKING IN 10 SECONDS");
                    String query = "" +
                            "SELECT si.server_host,di.port,di.username,di.password,di.database_name,tbls.`table_name`,\n" +
                            "jobs.id as job_id, str.partition_by, str.is_all, str.id as str_id,di.database_type, str.query,req.request_type,str.from_date,str.to_date,req.id as req_id FROM \n" +
                            "(SELECT * FROM webservice_test.jobs\n" +
                            "where number_retries < max_retries and status = 'pending' and deleted = 0) as jobs\n" +
                            "INNER JOIN\n" +
                            "(select is_all,id,request_id,partition_by,table_id, null as query,from_date,to_date from webservice_test.sync_table_requests \n" +
                            ") str \n" +
                            "INNER JOIN\n" +
                            "webservice_test.tables tbls\n" +
                            "INNER JOIN\n" +
                            "webservice_test.database_infos as di\n" +
                            "INNER JOIN\n" +
                            "webservice_test.server_infos as si\n" +
                            "INNER JOIN\n" +
                            "webservice_test.request as req\n" +
                            "on jobs.request_id = str.request_id\n" +
                            "and jobs.request_id = req.id\n" +
                            "and (str.table_id = tbls.id and si.id = di.server_info_id and tbls.database_info_id = di.id)\n" +
                            "union\n" +
                            "SELECT null as server_host,null as port,null as username,null as password,null as database_name,null as `table_name`,jobs.id as job_id, \n" +
                            "str.partition_by, str.is_all, str.id as str_id,null as database_type, str.query,req.request_type,null as from_date,null as to_date,req.id as req_id  FROM \n" +
                            "(\n" +
                            "select null as is_all,id,request_type_id as request_id,null as partition_by,null as table_id, query from webservice_test.etl_request\n" +
                            ") str \n" +
                            "INNER JOIN\n" +
                            "webservice_test.request as req\n" +
                            "INNER JOIN\n" +
                            "(SELECT * FROM webservice_test.jobs\n" +
                            "where number_retries < max_retries and status = 'pending' and deleted = 0) as jobs\n" +
                            "on jobs.request_id = str.request_id\n" +
                            "and jobs.request_id = req.id\n" +
                            "and req.request_type = 'ETLRequest';";

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
                        if (Objects.isNull(database_name) || database_name.equals("") || database_name.equals(" ")) {
                            // oracle case
                            database_name = username;
                        }
                        String table_name = rs.getString("table_name");
                        int job_id = rs.getInt("job_id");
                        int is_all = rs.getInt("is_all");
                        int str_id = rs.getInt("str_id");
                        String partition_by = rs.getString("partition_by");

                        if (Objects.isNull(partition_by) || partition_by.equals("") || partition_by.equals(" ")) {
                            partition_by = "' '";
                        }
                        String database_type = rs.getString("database_type");
                        String request_type = rs.getString("request_type");
                        String from_date = rs.getString("from_date");
                        if (Objects.isNull(from_date) || from_date.equals("") || from_date.equals(" ")) {
                            from_date = "' '";
                        }
                        String to_date = rs.getString("from_date");
                        if (Objects.isNull(to_date) || to_date.equals("") || to_date.equals(" ")) {
                            to_date = "' '";
                        }
                        String queryRequest = rs.getString("query");
                        if (Objects.isNull(queryRequest) || queryRequest.equals("") || queryRequest.equals(" ")) {
                            queryRequest = "' '";
                        } else {
                            queryRequest = String.format("'%s'", queryRequest);
                        }
                        String requestID = rs.getString("req_id");
                        String cmd = String.format(template, host, port, username, password, table_name, database_name,
                                partition_by, str_id, job_id, is_all, database_type, request_type, from_date, to_date, queryRequest, requestID);
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
