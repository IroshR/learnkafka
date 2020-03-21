package com.iroshnk.learnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class ConsumerKafkaManualOffset {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("enable.auto.commit", false);
        properties.put("group.id", "group1");

        ArrayList<String> topics = new ArrayList<>();
        topics.add("first-topic");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(topics);
            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {

                    System.out.println("Record read in KafkaConsumerApp : " + record.toString());

                    consumer.commitSync();
                }
            }

        } catch (Exception e) {
            System.out.println("Inside exception loop : ");
            e.printStackTrace();
        }
    }
}
