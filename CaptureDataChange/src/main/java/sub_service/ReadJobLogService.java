package sub_service;

import utils.kafka_utils.kafkaUtils;

public class ReadJobLogService {
    public static void main(String[] args) {
        System.out.println("READING JOBS LOG");
        kafkaUtils.messageListener("localhost:9092", "jobs_log");
    }
}
