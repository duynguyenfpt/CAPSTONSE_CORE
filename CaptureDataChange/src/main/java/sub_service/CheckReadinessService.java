package sub_service;

import models.TableMonitor;
import utils.db_utils.sqlUtils;
import utils.kafka_utils.kafkaUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CheckReadinessService {
    public static void main(String[] args) throws SQLException {
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        ConnectionSingleton cs = ConnectionSingleton.getInstance();
        Connection connection = cs.connection;
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
                    String query = "" +
                            "SELECT si.server_domain,si.server_host,di.`port`,di.database_type,str.identity_id,str.partition_by,str.id as str_id, jobs.id as job_id,\n" +
                            "jobs.max_retries, jobs.number_retries, tm.latest_offset, tm.`table`, tm.database, di.username FROM\n" +
                            "webservice_test.database_infos di\n" +
                            "inner join \n" +
                            "(select * from webservice_test.`tables`)tbls\n" +
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
                            "and tm.is_active = 1 and tm.is_ready = 1 " +
                            "and tm.database = di.database_name " +
                            "and tm.table = tbls.table_name " +
                            "and number_retries < max_retries and jobs.deleted = 0 and si.deleted = 0 and di.deleted = 0";
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
                        tm.setJob_id(rs.getInt("job_id"));
                        tm.setUsername(rs.getString("username"));
                        list_monitors.add(tm);
                    }
                    if (list_monitors.size() > 0) {
                        kafkaUtils.applicationProducer("localhost:9092", "cdc-table-change-test", list_monitors);
                    }
                } catch (SQLException exception) {
                    exception.printStackTrace();
                }
            }
        }, 0, 20, TimeUnit.SECONDS);
    }
}
