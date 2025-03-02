package org.kcd;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperativeStickyAssignor {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperativeStickyAssignor.class.getSimpleName());


        public static void main(String[] args) {
            log.info("Kafka Consumer is starting...");

            String groupId = "my-java-application";   //all consumers in the same group share the load
            String topic = "udemy-kafka-practise-topic";

            //create consumer properties to connect to docker compose
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092"); // Use localhost:29092 for external connection;
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            //partition reassignment startergy upon consumer leave or new adding
            properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "my-java-application-1");

            //create a consumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

            //get a reference to the main thread
            final Thread mainThread = Thread.currentThread();

            //adding the shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread() {    //register a shutdown hook
                public void run() {
                    log.info("Detected a sutdown, let's exit by calling consumer.wakeup().......");
                    consumer.wakeup();   //safely interrupt polling

                    //join the main thread to allow the execution of the code in the main thread
                    try{
                        mainThread.join();   //wait for the main thread to finish
                    } catch (InterruptedException e){
                        e.printStackTrace();
                    }
                }
            });
            try {

                //SUBSCRIBE TO TOPIC
                consumer.subscribe(Arrays.asList(topic));   //can subscribe to >1 topic

                //poll for data
                while (true) {

                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(1000)); //if no data in kafka waits for 1000ms till data is fetched
                    for (ConsumerRecord<String, String> record : records) {
                        log.info("Key: " + record.key() + ", Value: " + record.value());
                        log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }

            }catch(WakeupException e){
                log.info("Consumer is starting to shutdown...");
            } catch (Exception e) {
                log.error("unexpectedException in the consumer", e);
            }finally {
                consumer.close(); //close the consumer , this will also commit offsets
                log.info("Kafka Consumer is gracefully shutdown...");
            }
        }
    }

