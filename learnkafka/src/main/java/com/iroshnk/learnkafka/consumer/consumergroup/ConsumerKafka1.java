package com.iroshnk.learnkafka.consumer.consumergroup;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class ConsumerKafka1 {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092,localhost:9093");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "group1");

        ArrayList<String> topics = new ArrayList<>();
        topics.add("first-topic");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(topics);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Record read in KafkaConsumerApp 1 : " + record.toString());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
