/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.logging.appender.kafka;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.logging.LoggingConfiguration;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * A Kafka producer that publishes log messages to Kafka brokers.
 */
public final class SimpleKafkaProducer {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleKafkaProducer.class);

  private final String kafkaTopic;
  // Kafka producer is thread safe
  private final Producer<String, byte[]> producer;

  public SimpleKafkaProducer(CConfiguration configuration) {
    Properties props = new Properties();
    props.setProperty("metadata.broker.list", configuration.get(LoggingConfiguration.KAFKA_SEED_BROKERS));
    props.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
    props.setProperty("key.serializer.class", "kafka.serializer.StringEncoder");
    props.setProperty("partitioner.class", "co.cask.cdap.logging.appender.kafka.StringPartitioner");
    props.setProperty("request.required.acks", "1");
    props.setProperty("producer.type", configuration.get(LoggingConfiguration.KAFKA_PRODUCER_TYPE,
                                                         LoggingConfiguration.DEFAULT_KAFKA_PRODUCER_TYPE));
    props.setProperty("queue.buffering.max.ms",
                      configuration.get(LoggingConfiguration.KAFKA_PROCUDER_BUFFER_MS,
                                        Long.toString(LoggingConfiguration.DEFAULT_KAFKA_PROCUDER_BUFFER_MS)));
    props.setProperty(LoggingConfiguration.NUM_PARTITIONS,
                      configuration.get(LoggingConfiguration.NUM_PARTITIONS,
                                        LoggingConfiguration.DEFAULT_NUM_PARTITIONS));

    ProducerConfig config = new ProducerConfig(props);

    kafkaTopic = KafkaTopic.getTopic();
    producer = new Producer<String, byte[]>(config);
  }

  public void publish(String key, byte[] bytes) {
    try {
      KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>(kafkaTopic, key, bytes);
      producer.send(data);
    } catch (Throwable t) {
      LOG.error("Exception when trying to publish log message to kafka with key {} and topic {}", key, kafkaTopic, t);
    }
  }

  public void stop() {
    producer.close();
  }
}
