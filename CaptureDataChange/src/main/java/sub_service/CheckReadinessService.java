package sub_service;

import models.TableMonitor;
import utils.db_utils.sqlUtils;
import utils.kafka_utils.kafkaUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class CheckReadinessService {
    public static void main(String[] args) throws SQLException {
        Connection connection = sqlUtils.getConnection(
                sqlUtils.getConnectionString("localhost", "3306", "cdc", "duynt", "Capstone123@")
        );
        String query = "" +
                "SELECT si.server_domain,si.server_host,di.`port`,di.database_type,str.identity_id,str.partition_by,str.id as str_id, jobs.id as job_id,\n" +
                "jobs.max_retries, jobs.number_retries, tm.latest_offset, tm.`table`, tm.database FROM\n" +
                "webservice_test.database_infos di\n" +
                "inner join \n" +
                "(select * from webservice_test.`tables` where table_name = 'transacton' )tbls\n" +
                "inner join webservice_test.server_infos si\n" +
                "inner join webservice_test.sync_table_requests as str\n" +
                "inner join webservice_test.request \n" +
                "inner join webservice_test.jobs\n" +
                "inner join cdc.table_monitor as tm\n" +
                "on di.id = tbls.database_info_id\n" +
                "and di.server_info_id = si.id\n" +
                "and str.table_id = tbls.id\n" +
                "and str.request_id = request.id\n" +
                "and jobs.request_id = request.id\n" +
                "and (si.server_domain = tm.`host` or si.server_host = tm.`host`)\n" +
                "and di.`port` = tm.`port`\n" +
                "and tm.is_active = 1 and tm.is_ready = 1;";
        //
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(query);
        ArrayList<TableMonitor> list_monitors = new ArrayList<TableMonitor>();
        while (rs.next()) {
            TableMonitor tm = new TableMonitor();
            tm.setServer_host(rs.getString("server_host"));
            tm.setPort(rs.getString("port"));
            tm.setDatabase_type(rs.getString("database_type"));
            tm.setIdentity_key(rs.getString("identity_id"));
            tm.setPartition_by(rs.getString("partition_by"));
            tm.setTable(rs.getString("table"));
            tm.setStr_id(rs.getInt("str_id"));
            tm.setMax_retries(rs.getInt("max_retries"));
            tm.setNumber_retries(rs.getInt("number_retries"));
            tm.setLatest_offset(rs.getInt("latest_offset"));
            tm.setDatabase(rs.getString("database"));
            list_monitors.add(tm);
        }
        if (list_monitors.size() > 0) {
            kafkaUtils.applicationProducer("localhost:9092", "cdc-table-change-test", list_monitors);
        }
    }
}
