package test;

import utils.db_utils.sqlUtils;
import utils.kafka_utils.kafkaUtils;

import java.sql.Connection;

public class Test {
    public static void main(String[] args) {
        String host = "127.0.0.1";
        String port = "3307";
        String db = "test";
        String username = "root";
        String password = "Capstone123@";
//        String connectionString = sqlUtils.getConnectionString(host,port,db,username,password);
//        Connection connection = sqlUtils.getConnection(connectionString);
//        String deleteTrigger = sqlUtils.createTrigger(connection,host,port,db,"student",3);
//        System.out.println(deleteTrigger);
        kafkaUtils.createTopic("localhost:9092","test_create", 1,1);
    }
}
