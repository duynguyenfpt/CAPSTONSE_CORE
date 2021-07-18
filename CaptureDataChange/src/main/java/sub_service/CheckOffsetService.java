package sub_service;

import models.CDCModel;
import org.apache.kafka.common.protocol.types.Field;
import utils.db_utils.sqlUtils;
import utils.kafka_utils.kafkaUtils;

import java.sql.Connection;
import java.util.ArrayList;

public class CheckOffsetService {
    public static void main(String[] args) {
        String host = args[0];
        String port = args[1];
        String db = args[2];
        String username = args[3];
        String password = args[4];
        String table = args[5];
//        String connectionString = sqlUtils.getConnectionString(host, port, db, username, password);
//        Connection connection = sqlUtils.getConnection(connectionString);
        Connection connection = sqlUtils.getConnection(sqlUtils.getConnectionString("localhost", "3306",
                "cdc", "duynt", "Capstone123@"));
//        System.out.println(host + "-" + port + "-" + db + "-" + table);
        while (true) {
            int offset = sqlUtils.getOffsets(connection, db, host, port);
            int max_id = sqlUtils.getLatestID(connection);
            //
            if (offset < max_id) {
                //
                System.out.println("offsets: " + offset);
                System.out.println("max_id: " + max_id);
                //
                String topicName = "develop2-" + host + "-" + port + "-" + db + "-" + table;
                System.out.println(topicName);
                String kafkaCluster = "localhost:9092";
                //
                ArrayList<CDCModel> listCDCs = sqlUtils.getCDCs(connection, offset, max_id);
                //
                if (!kafkaUtils.checkExistTopic(kafkaCluster, topicName)) {
                    kafkaUtils.createTopic(kafkaCluster, topicName, 1, 1);
                }
                System.out.println("PRODUCING");
                // publish to kafka
                kafkaUtils.messageProducer(kafkaCluster, topicName, listCDCs);
                // update offset
                offset = max_id;
                // stay for student due to wrong design
                // update later
                sqlUtils.updateOffset(connection, db, host, port, offset);
            }
        }
    }
}
