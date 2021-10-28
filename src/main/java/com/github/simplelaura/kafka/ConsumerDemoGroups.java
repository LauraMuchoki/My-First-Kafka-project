package com.github.simplelaura.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoGroups {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        java.lang.String bootstrapServers = "127.0.0.1:9092";
        java.lang.String groupId = "my-first-application";
        java.lang.String topic = "fun-topic";

        //create the consumer configs

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe consumer to a topic(s)
        consumer.subscribe(Arrays.asList(topic));


        //poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("key: " + record.key() + ", Value: " + record.value());
                logger.info("partition: " + record.partition() + ", Offset: " + record.offset());
            }

        }

    }
}
