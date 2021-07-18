package utils.kafka_utils;

import com.google.gson.Gson;
import models.CDCModel;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class kafkaUtils {
    // check exists topic
    // if not then create new one
    // publish and subscribe messages
    public static boolean checkExistTopic(String kafkaCluster, String kafkaTopic) {
        Map<String, List<PartitionInfo>> topics;
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaCluster);
        props.put("group.id", "test-consumer-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        topics = consumer.listTopics();
        consumer.close();
        return topics.containsKey(kafkaTopic);
    }

    public static void createTopic(String kafkaCluster, String kafkaTopic, int partition, int replicationFactor) {
        if (replicationFactor > 1) {
            // currently only accept replication factor : 1
            // because there is only data node
            replicationFactor = 1;
        }
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaCluster);
        properties.put("connections.max.idle.ms", 10000);
        properties.put("request.timeout.ms", 5000);
        try {
            AdminClient client = AdminClient.create(properties);
            CreateTopicsResult result = client.createTopics(Arrays.asList(
                    new NewTopic(kafkaTopic, partition, (short) replicationFactor)
            ));
            try {
                result.all().get();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        } catch (Exception exception){

        }
    }

    public static void messageListener(String kafkaCluster, String kafkaTopic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        // đọc các message của topic từ thời điểm hiển tại (default)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // đọc tất cả các message  của topic từ offset ban đầu
        //    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(props);
        consumer.subscribe(Arrays.asList(kafkaTopic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }

    public static void messageProducer(String kafkaCluster, String kafkaTopic, ArrayList<CDCModel> listCDCs) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //If the request fails, the producer can automatically retry,
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //Reduce the no of requests less than 0
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        Gson gson = new Gson();
        System.out.println(kafkaTopic);
        for (CDCModel cdcModel : listCDCs) {
            producer.send(new ProducerRecord<String, String>(kafkaTopic, cdcModel.getDatabase_url() + "-" + cdcModel.getDatabase_port()
                    + "-" + cdcModel.getDatabase_name() + "-" + cdcModel.getTable_name(), gson.toJson(cdcModel)));
            System.out.println("producing: " + gson.toJson(cdcModel));
        }
        producer.close();
        System.out.println("DONE");
    }
}
