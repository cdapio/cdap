package com.continuuity.common.logging.logback.kafka;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.common.conf.CConfiguration;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * A Kafka producer that publishes log messages to Kafka brokers.
 */
public class SimpleKafkaProducer {
  private static final String KAFKA_TOPIC = "LOG_MESSAGES";
  private Producer<String, ILoggingEvent> producer;

  public SimpleKafkaProducer(CConfiguration configuration) {
    Properties props = new Properties();
    props.setProperty("metadata.broker.list", "localhost:9092,localhost:9093");
    props.setProperty("serializer.class", "com.continuuity.common.logging.logback.kafka.LoggingEventSerializer");
    props.setProperty("key.serializer.class", "kafka.serializer.StringEncoder");
    props.setProperty("partitioner.class", "com.continuuity.common.logging.logback.kafka.StringPartitioner");
    props.setProperty("request.required.acks", "1");

    ProducerConfig config = new ProducerConfig(props);

    producer = new Producer<String, ILoggingEvent>(config);
  }

  public void publish(String key, ILoggingEvent event) {
    KeyedMessage<String, ILoggingEvent> data = new KeyedMessage<String, ILoggingEvent>(KAFKA_TOPIC, key, event);
    producer.send(data);
  }

  public void stop() {
    producer.close();
  }
}
