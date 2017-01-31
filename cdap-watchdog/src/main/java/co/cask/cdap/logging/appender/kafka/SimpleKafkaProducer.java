/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.logging.LoggingConfiguration;
import com.google.common.util.concurrent.Futures;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A Kafka producer that publishes log messages to Kafka brokers.
 */
public final class SimpleKafkaProducer {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleKafkaProducer.class);

  private final String kafkaTopic;
  // Kafka producer is thread safe
  private final Producer<String, byte[]> producer;

  public SimpleKafkaProducer(CConfiguration cConf) {
    Properties props = new Properties();
    props.setProperty("metadata.broker.list", cConf.get(LoggingConfiguration.KAFKA_SEED_BROKERS));
    props.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
    props.setProperty("key.serializer.class", "kafka.serializer.StringEncoder");
    props.setProperty("partitioner.class", "co.cask.cdap.logging.appender.kafka.StringPartitioner");
    props.setProperty("request.required.acks", "1");
    props.setProperty("producer.type", cConf.get(LoggingConfiguration.KAFKA_PRODUCER_TYPE,
                       LoggingConfiguration.DEFAULT_KAFKA_PRODUCER_TYPE));
    props.setProperty("queue.buffering.max.ms", cConf.get(LoggingConfiguration.KAFKA_PRODUCER_BUFFER_MS,
                      Long.toString(LoggingConfiguration.DEFAULT_KAFKA_PRODUCER_BUFFER_MS)));
    props.setProperty(Constants.Logging.NUM_PARTITIONS, cConf.get(Constants.Logging.NUM_PARTITIONS));

    ProducerConfig config = new ProducerConfig(props);
    kafkaTopic = cConf.get(Constants.Logging.KAFKA_TOPIC);
    producer = createProducer(config);
  }

  public void publish(String key, byte[] bytes) {
    // Clear the interrupt flag, otherwise it won't be able to publish
    boolean threadInterrupted = Thread.interrupted();
    try {
      KeyedMessage<String, byte[]> data = new KeyedMessage<>(kafkaTopic, key, bytes);
      producer.send(data);
    } catch (Throwable t) {
      LOG.error("Exception when trying to publish log message to kafka with key {} and topic {}", key, kafkaTopic, t);
    } finally {
      // Reset the interrupt flag if needed
      if (threadInterrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public void stop() {
    producer.close();
  }

  /**
   * Creates a {@link Producer} using the given configuration. The producer instance will be created from a
   * daemon thread to make sure the async thread created inside Kafka is also a daemon thread.
   */
  private <K, V> Producer<K, V> createProducer(final ProducerConfig config) {
    ExecutorService executor = Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("create-producer"));
    try {
      return Futures.getUnchecked(executor.submit(new Callable<Producer<K, V>>() {
        @Override
        public Producer<K, V> call() throws Exception {
          return new Producer<>(config);
        }
      }));
    } finally {
      executor.shutdownNow();
    }
  }
}
