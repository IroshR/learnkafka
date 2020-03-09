package com.iroshnk.learnkafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerKafka {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("partitioner.class", "com.iroshnk.learnkafka.partitioner.ProducerKafkaPartitioner");

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {

            for (int i = 1; i < 250; i++) {
                kafkaProducer.send(new ProducerRecord<String, String>("first-topic", 0, "message 1", "Message Value : " + Integer.toString(i)));
//                kafkaProducer.send(new  ProducerRecord<>("my-fifth-topic", "url:<local-directory-path>/file"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
