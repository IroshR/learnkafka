package com.iroshnk.learnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class ConsumerKafkaResetOffset {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092,localhost:9093");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test1");

        String topic = "first-topic";

        ArrayList<String> topics = new ArrayList<>();
        topics.add(topic);

        TopicPartition partition = new TopicPartition(topic, 0);
        boolean flag = false;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(topics);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                if (!flag) {
                    consumer.seek(partition, 0);
                    flag = true;
                }
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Record read in KafkaConsumerApp : " + record.toString());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
