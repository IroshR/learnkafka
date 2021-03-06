package com.iroshnk.learnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class ConsumerKafka {
    public static void main(String[] args) {

        Properties properties=new Properties();
        properties.put("bootstrap.servers", "localhost:9092,localhost:9093");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id","test1");

        //for secured topic
        /*properties.put("security.protocol","SSL");
        properties.put("ssl.truststore.location","<path>/server.truststore.jks");
        properties.put("ssl.truststore.password","kafka123");
        properties.put("ssl.keystore.location","<path>/server.keystore.jks");
        properties.put("ssl.keystore.password","kafka123");
        properties.put("ssl.key.password","kafka123");*/



        ArrayList<String> topics=new ArrayList<>();
        topics.add("first-topic");

        try (KafkaConsumer< String, String> consumer=new KafkaConsumer<>(properties)){
            consumer.subscribe(topics);
            while(true){
                ConsumerRecords<String, String> records = consumer.poll( Duration.ofMillis(1000));
                for(ConsumerRecord<String, String> record : records){
                    System.out.println("Record read in KafkaConsumerApp : " +  record.toString());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
