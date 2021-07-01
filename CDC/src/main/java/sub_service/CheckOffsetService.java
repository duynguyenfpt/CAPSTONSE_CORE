package sub_service;

import models.CDCModel;
import utils.db_utils.sqlUtils;
import utils.kafka_utils.kafkaUtils;

import java.sql.Connection;
import java.util.ArrayList;

public class CheckOffsetService {
    public static void main(String[] args) {
        String host = "127.0.0.1";
        String port = "3307";
        String db = "test";
        String username = "root";
        String password = "Capstone123@";
        String table = "student";
        String connectionString = sqlUtils.getConnectionString(host, port, db, username, password);
        Connection connection = sqlUtils.getConnection(connectionString);
//        System.out.println(host + "-" + port + "-" + db + "-" + table);
        while (true){
            int offset = sqlUtils.getOffsets(connection,db,table);
            int max_id = sqlUtils.getLatestID(connection);
            //
            if (offset < max_id) {
                //
                System.out.println("offsets: " + offset);
                System.out.println("max_id: " + max_id);
                //
                ArrayList<CDCModel> listCDCs = sqlUtils.getCDCs(connection,offset,max_id);
                // publish to kafka
                kafkaUtils.messageProducer("localhost:9092", host+"-"+port+"-"+db+"-"+table,listCDCs);
                // update offset
                offset = max_id;
                sqlUtils.updateOffset(connection,db,table,offset);
            }
        }
    }
}
