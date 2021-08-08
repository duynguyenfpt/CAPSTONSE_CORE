package com.bigdata.capstone;

import utils.db_utils.sqlUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

public class Test {
    public static void main(String[] args) throws SQLException {
        String query = "select distinct database_host, database_port,username,password\n" +
                "from cdc.offsets as os\n" +
                "inner join webservice_test.database_infos as di\n" +
                "inner join webservice_test.server_infos as si\n" +
                "on os.database_port = di.port\n" +
                "and si.id = di.server_info_id\n" +
                "and (si.server_domain = database_host or si.server_host = database_host) " +
                "and si.deleted = 0 and di.deleted =0 ;";
        System.out.println(query);
    }
}
