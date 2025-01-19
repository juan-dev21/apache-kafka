package com.jp21.apache.kafka.session01.multithread;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumerThread extends Thread {

  private final static Logger log = LoggerFactory.getLogger(KafkaConsumerThread.class);
  private final KafkaConsumer<String, String> consumer;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public KafkaConsumerThread(KafkaConsumer<String, String> consumer) {
    this.consumer = consumer;
  }

  @Override
  public void run() {
    consumer.subscribe(Collections.singletonList("my-topic"));

      try {
        while (!closed.get()) {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
          for (ConsumerRecord<String, String> record : records) {
            log.info("offset = {}, partition = {}, key = {}, value = {}", record.offset(), record.partition(), record.key(), record.value());
          }
        }
      } catch (WakeupException e) {
        log.error("Error consuming messages", e);
        throw e;
      } finally {
        consumer.close();
      }

  }



  public void shutdown() {
    closed.set(true);
    consumer.wakeup();
  }


}
