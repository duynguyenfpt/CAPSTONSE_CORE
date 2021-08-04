package sub_service;

import models.CDCModel;
import models.OffsetModel;
import org.apache.kafka.common.protocol.types.Field;
import utils.db_utils.sqlUtils;
import utils.kafka_utils.kafkaUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CheckOffsetService {
    public static final String prefix = "cdc_4912929_";

    public static void handle(String host, String port, String username, String password, Connection offsetConnection) throws SQLException {
//        String table = args[5];
        // connect to database cdc of data source
        String cdcDB = String.format("%s_cdc", prefix);
        String cdcTable = "cdc_detail";
        //
        OffsetModel offset = sqlUtils.getOffsets(offsetConnection, host, port);
        //
        Connection sourceConnection = null;
        if (offset.getDbType().equalsIgnoreCase("mysql") || offset.getDbType().equalsIgnoreCase("postgresql")) {
            sourceConnection = sqlUtils.getConnection(sqlUtils.getConnectionString(host, port,
                    cdcDB, username, password, offset.getDbType()));
        } else {
            String sid = sqlUtils.getSID(host, port, offsetConnection);
            sourceConnection = sqlUtils.getConnectionOracle(username, password, host, port, sid);
        }
        int max_id = sqlUtils.getLatestID(sourceConnection);
        //
        if (offset.getOffsets() < max_id) {
            //
            System.out.println("offsets: " + offset);
            System.out.println("max_id: " + max_id);
            //
            //
            ArrayList<CDCModel> listCDCs = sqlUtils.getCDCs(sourceConnection, offset.getOffsets(), max_id);
            //
            String current_table = "";
            ArrayList current_cdcs = new ArrayList();
            int index = 0;
            for (CDCModel cdc : listCDCs) {
                String table = cdc.getTable_name();
                // check if current one is blank
                // only case is the first cdc
                if (current_table.equals("")) {
                    current_table = table;
                }
                //
                if (!current_table.equalsIgnoreCase(table)) {
                    //
                    sendCDC(host, port, cdc.getDatabase_name(), current_table, current_cdcs, offsetConnection);
                    System.out.println(String.format("sending when index is : %s", index));
                    // re-assign
                    current_table = table;
                    current_cdcs = new ArrayList();
                }
                current_cdcs.add(cdc);
                if (index == listCDCs.size() - 1) {
                    // case when the last is the only one
                    sendCDC(host, port, cdc.getDatabase_name(), current_table, current_cdcs, offsetConnection);
                    System.out.println(String.format("sending when index is : %s", index));
                }
                index++;
            }
            sqlUtils.updateOffset(offsetConnection, host, port, max_id);
        }
    }

    public static void main(String[] args) {
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        Connection offsetConnection = sqlUtils.getConnection(sqlUtils.getConnectionString("localhost", "3306",
                "cdc", "duynt", "Capstone123@"));

        exec.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                Date date = new Date();
                System.out.println("START FETCHING ALL DATABASE SOURCES: " + dateFormat.format(date));
                System.out.println("NEXT FETCHING IN 10 SECONDS");
                Connection configConnection = sqlUtils.getConnection(sqlUtils.getConnectionString("localhost", "3306",
                        "cdc", "duynt", "Capstone123@"));
                Statement stmt = null;
                try {
                    stmt = configConnection.createStatement();
                    ResultSet rs = stmt.executeQuery("" +
                            "select distinct database_host, database_port,username,password\n" +
                            "from cdc.offsets as os\n" +
                            "inner join webservice_test.database_infos as di\n" +
                            "inner join webservice_test.server_infos as si\n" +
                            "on os.database_port = di.port\n" +
                            "and si.id = di.server_info_id\n" +
                            "and (si.server_domain = database_host or si.server_host = database_host);");
                    while (rs.next()) {
                        String host = rs.getString("database_host");
                        String port = rs.getString("database_port");
                        String username = rs.getString("username");
                        String password = rs.getString("password");
                        handle(host, port, username, password, offsetConnection);
                    }
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                }
            }
        }, 0, 10, TimeUnit.SECONDS);

    }

    public static void sendCDC(String host, String port, String db, String current_table,
                               ArrayList<CDCModel> current_cdcs, Connection connection) throws SQLException {
        String topicName = "cdc-" + host + "-" + port + "-" + db + "-" + current_table;
        System.out.println(topicName);
        String kafkaCluster = "localhost:9092";
        //
        if (!kafkaUtils.checkExistTopic(kafkaCluster, topicName)) {
            kafkaUtils.createTopic(kafkaCluster, topicName, 1, 1);
        }
        System.out.println("PRODUCING");
        // publish to kafka
        kafkaUtils.messageProducer(kafkaCluster, topicName, current_cdcs);
        // stay for student due to wrong design
        // update later
        sqlUtils.updateReady(host, port, db, current_table, connection, 1);
    }
}
