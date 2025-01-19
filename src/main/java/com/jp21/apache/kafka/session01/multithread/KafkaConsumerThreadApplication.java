package com.jp21.apache.kafka.session01.multithread;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaConsumerThreadApplication {
    public static void main(String[] args) {
      Properties properties = new Properties();
      properties.setProperty("bootstrap.servers", "localhost:9092");
      properties.setProperty("group.id", "jp.group-consumer");
      properties.setProperty("enable.auto.commit", "true"); // Para configurar si el consumidor realiza un commit automático de los offsets que ha consumido
      properties.setProperty("auto.commit.interval.ms", "1000"); // Para configurar cada cuánto tiempo realiza el consumidor un commit automático de los offsets que ha consumido
      properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


      ExecutorService executorService = Executors.newFixedThreadPool(5);
      for (int i = 0; i < 5; i++) {
        KafkaConsumerThread kafkaConsumerThread = new KafkaConsumerThread(new KafkaConsumer<>(properties));
        executorService.execute(kafkaConsumerThread);
      }
      while (executorService.isTerminated()) {
        executorService.shutdown();
      }
    }
}
