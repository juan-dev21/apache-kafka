package com.jp21.apache.kafka.session01;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerApplication {

    private final static Logger log = LoggerFactory.getLogger(KafkaProducerApplication.class);

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all"); // 0, 1, all [no ack, leader ack, all ack]
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("linger.ms", "15");

        try (Producer<String, String> producer = new KafkaProducer<>(properties);) {

            for (int i = 0; i < 1_000_000; i++) {
                log.info("Sending message: " + i);
                producer.send(new ProducerRecord<>("my-topic", Integer.toString(i), Integer.toString(i)));
            }
            producer.flush();
        }

        log.info("Time taken: " + (System.currentTimeMillis() - startTime) + " ms");
        // 4994 ms
        // 3604 ms | linger 6
        // 3711 ms | linger 4
        // 3611 ms | linger 10
        // 3356 ms | linger 15

    }
}
