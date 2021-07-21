package sub_service;

import models.CDCModel;
import org.apache.kafka.common.protocol.types.Field;
import utils.db_utils.sqlUtils;
import utils.kafka_utils.kafkaUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class CheckOffsetService {
    public static final String prefix = "cdc_4912929_";

    public static void handle(String host, String port, String username, String password) throws SQLException {
//        String table = args[5];
        // connect to database cdc of data source
        String cdcDB = String.format("%s_cdc", prefix);
        String cdcTable = "cdc_detail";
        //
        Connection offsetConnection = sqlUtils.getConnection(sqlUtils.getConnectionString("localhost", "3306",
                "cdc", "duynt", "Capstone123@"));
        Connection sourceConnection = sqlUtils.getConnection(sqlUtils.getConnectionString(host, port,
                cdcDB, username, password));
        while (true) {
            int offset = sqlUtils.getOffsets(offsetConnection, host, port);
            int max_id = sqlUtils.getLatestID(sourceConnection);
            //
            if (offset < max_id) {
                //
                System.out.println("offsets: " + offset);
                System.out.println("max_id: " + max_id);
                //
                //
                ArrayList<CDCModel> listCDCs = sqlUtils.getCDCs(sourceConnection, offset, max_id);
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
                        sendCDC(host, port, cdc.getDatabase_name(), current_table, current_cdcs, max_id, offsetConnection);
                        System.out.println(String.format("sending when index is : %s", index));
                        // re-assign
                        current_table = table;
                        current_cdcs = new ArrayList();
                    }
                    current_cdcs.add(cdc);
                    if (index == listCDCs.size() - 1) {
                        // case when the last is the only one
                        sendCDC(host, port, cdc.getDatabase_name(), current_table, current_cdcs, max_id, offsetConnection);
                        System.out.println(String.format("sending when index is : %s", index));
                    }
                    index++;
                }
            }
        }
    }

    public static void main(String[] args) throws SQLException {
        Connection configConnection = sqlUtils.getConnection(sqlUtils.getConnectionString("localhost", "3306",
                "cdc", "duynt", "Capstone123@"));
        Statement stmt = configConnection.createStatement();
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
            handle(host, port, username, password);
        }
    }

    public static void sendCDC(String host, String port, String db, String current_table,
                               ArrayList<CDCModel> current_cdcs, int max_id, Connection connection) throws SQLException {
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
        sqlUtils.updateOffset(connection, db, host, port, max_id);
        sqlUtils.updateReady(host, port, db, current_table, connection, 1);
    }
}
