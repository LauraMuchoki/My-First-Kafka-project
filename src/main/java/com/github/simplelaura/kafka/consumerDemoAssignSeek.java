package com.github.simplelaura.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        java.lang.String bootstrapServers = "127.0.0.1:9092";

        java.lang.String topic = "fun-topic";

        //create the consumer configs

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Assign and seek are mostly used to replay data or fetch a specific message

        //Assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //Seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean KeepOnReading = true;
        int numberOfMessagesReadSoFar = 0;


        //poll for new data
        while (KeepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesReadSoFar += 1;
                logger.info("key: " + record.key() + ", Value: " + record.value());
                logger.info("partition: " + record.partition() + ", Offset: " + record.offset());
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    KeepOnReading = false;  //to exit the while loop
                    break;  //to exit the for loop
                }
            }

        }
        logger.info("Exiting the application");
    }
}


