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
//        try {
//            Connection configConnection = sqlUtils.getConnection(sqlUtils.getConnectionString("localhost", "3306",
//                    "cdc", "duynt", "Capstone123@"));
//        } catch (Exception ex) {
//            ex.printStackTrace();
//        }
//        String query = "" +
//                "SELECT si.server_domain,si.server_host,di.`port`,di.database_type,str.identity_id,str.partition_by,str.id as str_id, jobs.id as job_id,\n" +
//                "jobs.max_retries, jobs.number_retries, tm.latest_offset, tm.`table`, tm.database FROM\n" +
//                "webservice_test.database_infos di\n" +
//                "inner join \n" +
//                "(select * from webservice_test.`tables`)tbls\n" +
//                "inner join webservice_test.server_infos si\n" +
//                "inner join webservice_test.sync_table_requests as str\n" +
//                "inner join webservice_test.request \n" +
//                "inner join webservice_test.jobs\n" +
//                "inner join cdc.table_monitor as tm\n" +
//                "on di.id = tbls.database_info_id\n" +
//                "and di.server_info_id = si.id\n" +
//                "and str.table_id = tbls.id\n" +
//                "and str.request_id = request.id\n" +
//                "and jobs.request_id = request.id\n" +
//                "and (si.server_domain = tm.`host` or si.server_host = tm.`host`)\n" +
//                "and di.`port` = tm.`port`\n" +
//                "and tm.is_active = 1 and tm.is_ready = 1 " +
//                "and tm.database = di.database_name " +
//                "and tm.table = tbls.table_name " +
//                "and number_retries < max_retries and jobs.deleted = 0";
//        System.out.println(query);
        String createTable = "" +
                "CREATE TABLE IF NOT EXISTS public.cdc_detail (\n" +
                "  id SERIAL,\n" +
                "  database_url varchar(45) DEFAULT NULL,\n" +
                "  database_port varchar(6) DEFAULT NULL,\n" +
                "  database_name varchar(20) DEFAULT NULL,\n" +
                "  table_name varchar(20) DEFAULT NULL,\n" +
                "  schema varchar(200) DEFAULT NULL,\n" +
                "  value varchar(200) DEFAULT NULL,\n" +
                "  operation int DEFAULT NULL,\n" +
                "  created date DEFAULT CURRENT_DATE\n" +
                ");";
        System.out.println(createTable);
    }
}
