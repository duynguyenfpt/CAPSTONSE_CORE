package sub_service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import models.MergeRequest;
import models.TableModel;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.net.URI;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CheckMergeRequest {
    public static void main(String[] args) throws SQLException {
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        ConnectionSingleton cs = ConnectionSingleton.getInstance();
        Connection connection = cs.connection;
        Gson gson = new Gson();
        exec.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                Date date = new Date();
                System.out.println("START FETCHING ALL DATABASE SOURCES: " + dateFormat.format(date));
                System.out.println("NEXT FETCHING IN 20 SECONDS");
                try {
                    // get valid job and request
                    // ready tables
                    Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT latest_metadata, current_metadata,request_type_id" +
                            " FROM webservice_test.merge_requests" +
                            " WHERE latest_metadata <> current_metadata or current_metadata is null");
                    while (rs.next()) {
                        String latest_metadata = rs.getString("latest_metadata");
                        String current_metadata = rs.getString("current_metadata");
                        int requestMergeID = rs.getInt("request_type_id");
                        MergeRequest new_metadata = new ObjectMapper().readValue(latest_metadata, MergeRequest.class);
                        MergeRequest old_metadata = null;
                        ArrayList<Integer> listTablesChange = new ArrayList<>();
                        if (current_metadata != null) {
                            old_metadata = new ObjectMapper().readValue(current_metadata, MergeRequest.class);
                            for (TableModel tm : new_metadata.getList_tables()) {
                                boolean isNew = true;
                                for (TableModel tm2 : old_metadata.getList_tables()) {
                                    if (tm2.getTable_id() == tm.getTable_id()) {
                                        isNew = false;
                                        break;
                                    }
                                }
                                if (isNew) {
                                    listTablesChange.add(tm.getTable_id());
                                }
                            }
                        } else {
                            for (TableModel tm : new_metadata.getList_tables()) {
                                listTablesChange.add(tm.getTable_id());
                            }
                        }
                        for (int index = 0; index < listTablesChange.size(); index++) {
                            String pathToHDFSFolder = getPath(connection, listTablesChange.get(index));
                            System.out.println(pathToHDFSFolder);
                            if (!checkPath(pathToHDFSFolder)) {
                                // build a request sync
                                System.out.println(listTablesChange.get(index));
                                createSyncRequest(connection, listTablesChange.get(index), requestMergeID);
                            }
                        }
                        createJobs(connection, requestMergeID);
                    }
                } catch (Exception exception) {
                    exception.printStackTrace();
                }
            }
        }, 0, 20, TimeUnit.SECONDS);
    }


    private static void createSyncRequest(Connection connection, int tableID, int requestID) throws SQLException {
        // insert sync_table_request table first
        String insertSTR = "Insert into webservice_test.sync_table_requests(is_all,table_id,request_id) " +
                "values (0,?,?)";
        PreparedStatement prpStmt = connection.prepareStatement(insertSTR);
        prpStmt.setInt(1, tableID);
        prpStmt.setInt(2, requestID);
        prpStmt.executeUpdate();
        System.out.println("insert sync_table_request successfully");
    }

    private static void createJobs(Connection connection, int requestID) throws SQLException {
        // insert jobs table
        String insertJobs = "Insert into webservice_test.jobs(is_active,max_retries,executed_by,created_by,request_id,status,number_retries,deleted) " +
                "values (1,10,20,'longvt',?,'pending',0,0)";
        PreparedStatement prpStmt2 = connection.prepareStatement(insertJobs);
        prpStmt2.setInt(1, requestID);
        prpStmt2.executeUpdate();
        System.out.println("insert jobs successfully");
    }

    private static Boolean checkPath(String uri) {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            FileStatus[] fileStatuses = fs.globStatus(new Path(uri));
            if (fileStatuses == null) return false;
            // Check if folder exists at the given location
            return fileStatuses.length > 0;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static String getPath(Connection connection, int tableId) throws SQLException {
        String query = "SELECT si.server_host,di.port," +
                "di.database_type,di.database_name,username,table_name FROM webservice_test.database_infos di\n" +
                "INNER JOIN\n" +
                "webservice_test.server_infos si \n" +
                "INNER JOIN\n" +
                "webservice_test.tables tbls\n" +
                "on di.server_info_id = si.id\n" +
                "and tbls.database_info_id = di.id\n" +
                "and tbls.id = ?;";
        PreparedStatement prpStmt = connection.prepareStatement(query);
        prpStmt.setInt(1, tableId);
        ResultSet rs = prpStmt.executeQuery();
        while (rs.next()) {
            String host = rs.getString("server_host");
            String port = rs.getString("port");
            String databaseType = rs.getString("database_type");
            String databaseName = rs.getString("database_name");
            String username = rs.getString("username");
            String table = rs.getString("table_name");
            String path = "";
            if (!databaseType.equals("oracle")) {
                path = String.format("/user/%s/%s/%s/%s/%s/", databaseType, host, port, databaseName, table);
            } else {
                path = String.format("/user/%s/%s/%s/%s/%s/", databaseType, host, port, username, table);
            }
            return path;
        }
        return "";
    }
}
