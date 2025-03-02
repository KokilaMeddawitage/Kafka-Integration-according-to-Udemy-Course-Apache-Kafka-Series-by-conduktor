package org.kcd;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;


public class ProducerDemoWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoUsingStickyPartitioner.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka Producer is starting...");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092"); // Use localhost:29092 for external connection);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(400)); // 32KB batch size

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    for (int j=0; j<2;j++) {
        for (int i = 0; i < 10; i++) {

            String topic = "udemy-kafka-practise-topic";
            String key = "id_" + i;
            String value = "Hello World" + i;

            //create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            // Send data asynchronously
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    log.info("Message sent successfully to topic {}", metadata.topic());
                    log.info("key {}", key);
                    log.info("partition {}", metadata.partition());
                    log.info("offset {}", metadata.offset());
                } else {
                    log.error("Error while producing message", exception);
                }
            });
        }
    }


        //flush and close the producer
        producer.flush();
        producer.close();

        log.info("Kafka Producer finished.");
    }
}
