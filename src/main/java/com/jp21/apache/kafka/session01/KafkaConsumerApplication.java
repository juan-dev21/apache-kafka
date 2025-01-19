package com.jp21.apache.kafka.session01;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerApplication {

  private final static Logger log = LoggerFactory.getLogger(KafkaConsumerApplication.class);

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "jp.group-consumer");
    properties.setProperty("enable.auto.commit", "true"); // Para configurar si el consumidor realiza un commit automático de los offsets que ha consumido
    properties.setProperty("auto.offset.reset", "earliest"); // Para configurar qué hacer cuando no hay un offset inicial o el offset no existe en el servidor
    properties.setProperty("auto.commit.interval.ms", "1000"); // Para configurar cada cuánto tiempo realiza el consumidor un commit automático de los offsets que ha consumido
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
      consumer.subscribe(Collections.singletonList("my-topic"));

      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : records) {
          log.info("offset = {}, partition = {}, key = {}, value = {}", record.offset(), record.partition(), record.key(), record.value());
        }
      }
    }
  }
}
