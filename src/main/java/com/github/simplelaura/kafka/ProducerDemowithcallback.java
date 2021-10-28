package com.github.simplelaura.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemowithcallback {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemowithcallback.class);
        java.lang.String bootstrapServers = "127.0.0.1:9092";

        //create producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        for (int i = 0; i < 10; i++) {
            //create a producer
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

            //create a record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("fun-topic", "Hello world" + i);

            //Send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes everytime a record is successfully sent or an execption is thrown
                    if (e == null) {
                        //the record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "Timestamp:" + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing");
                    }
                }

            });

            //flush data
            producer.flush();

            //flush and close producer
            producer.close();

        }
    }
}