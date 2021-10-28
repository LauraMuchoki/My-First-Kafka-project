package com.github.simplelaura.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }






        private ConsumerDemoWithThread() {

        }

        private void run(){
            Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());
            java.lang.String bootstrapServers = "127.0.0.1:9092";
            java.lang.String groupId = "my-sixth-application";
            java.lang.String topic = "fun-topic";


            // latch for dealing with multiple Threads
            CountDownLatch latch = new CountDownLatch(1);


            // Create the consumer runnable
            logger.info("Creating the consumer thread");
            Runnable myConsumerRunnable = new ConsumerThread(
                    bootstrapServers,
                    groupId,
                    topic,
                    latch
                    );

            //start the thread
            Thread myThread = new Thread(myConsumerRunnable);
            myThread.start();


            //add a shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread( () -> {
                logger.info("Caught shutdown hook");
                ((ConsumerThread) myConsumerRunnable). shutdown();
            }
            ));

            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("Application got interrupted", e);
            }finally {
                logger.info("Application is closing");
            }
            }


        }

        class ConsumerThread implements Runnable {

            private CountDownLatch latch;
            private KafkaConsumer<String, String> consumer;

            private Logger logger = LoggerFactory.getLogger(com.github.simplelaura.kafka.ConsumerDemo.class.getName());

            public ConsumerThread(String bootstrapserver, String groupId, String topic, CountDownLatch latch) {
                this.latch = latch;

                //create the consumer configs

                Properties properties = new Properties();
                properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
                properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
                properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

                //create a consumer
                consumer = new KafkaConsumer<String, String>(properties);

                //subscribe consumer to a topic(s)
                consumer.subscribe(Arrays.asList(topic));

            }

            @Override
            public void run() {
                //poll for new data
                try {
                    while (true) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                        for (ConsumerRecord<String, String> record : records) {
                            logger.info("key: " + record.key() + ", Value: " + record.value());
                            logger.info("partition: " + record.partition() + ", Offset: " + record.offset());


                        }
                    }
                } catch (WakeupException e) {
                    logger.info("Received shutdown signal!");

                } finally {
                    consumer.close();
                    //tell our main code we are done with the consumer
                    latch.countDown();
                }
            }

                public void shutdown() {
                //the WakeUp()method ia a special method to interrupt consumer.poll()
                    //it will throw the exception WakeUpException
                    consumer.wakeup();
                }

        }



