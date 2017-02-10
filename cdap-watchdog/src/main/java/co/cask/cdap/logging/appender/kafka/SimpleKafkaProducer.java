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
import com.google.common.util.concurrent.SettableFuture;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.DefaultEncoder;
import kafka.serializer.StringEncoder;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.kafka.client.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A Kafka producer that publishes log messages to Kafka brokers.
 */
final class SimpleKafkaProducer {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleKafkaProducer.class);

  private static final long PRODUCER_CREATION_TIMEOUT_SECS = 2L;

  private final Properties producerProperties;
  private final BrokerService brokerService;
  private final String kafkaTopic;

  // Kafka producer is thread safe
  private volatile Producer<String, byte[]> producer;

  SimpleKafkaProducer(CConfiguration cConf, BrokerService brokerService) {
    Properties props = new Properties();
    props.setProperty("serializer.class", DefaultEncoder.class.getName());
    props.setProperty("key.serializer.class", StringEncoder.class.getName());
    props.setProperty("partitioner.class", StringPartitioner.class.getName());
    props.setProperty("request.required.acks", "1");
    props.setProperty("producer.type", cConf.get(LoggingConfiguration.KAFKA_PRODUCER_TYPE,
                       LoggingConfiguration.DEFAULT_KAFKA_PRODUCER_TYPE));
    props.setProperty("queue.buffering.max.ms", cConf.get(LoggingConfiguration.KAFKA_PRODUCER_BUFFER_MS,
                      Long.toString(LoggingConfiguration.DEFAULT_KAFKA_PRODUCER_BUFFER_MS)));
    props.setProperty(Constants.Logging.NUM_PARTITIONS, cConf.get(Constants.Logging.NUM_PARTITIONS));

    this.producerProperties = props;
    this.brokerService = brokerService;
    this.kafkaTopic = cConf.get(Constants.Logging.KAFKA_TOPIC);
  }

  void publish(String key, byte[] bytes) {
    // Clear the interrupt flag, otherwise it won't be able to publish
    boolean threadInterrupted = Thread.interrupted();
    try {
      KeyedMessage<String, byte[]> data = new KeyedMessage<>(kafkaTopic, key, bytes);
      getProducer().send(data);
    } catch (Throwable t) {
      LOG.error("Exception when trying to publish log message to kafka with key {} and topic {}", key, kafkaTopic, t);
    } finally {
      // Reset the interrupt flag if needed
      if (threadInterrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  void stop() {
    Producer<String, byte[]> producer = this.producer;
    if (producer != null) {
      producer.close();
    }
  }

  /**
   * Returns a {@link Producer} for writing to Kafka.
   */
  private Producer<String, byte[]> getProducer() throws InterruptedException, ExecutionException, TimeoutException {
    if (producer != null) {
      return producer;
    }
    synchronized (this) {
      if (producer != null) {
        return producer;
      }
      producer = createProducer();
    }
    return producer;
  }

  /**
   * Creates a {@link Producer} using the given configuration. The producer instance will be created from a
   * daemon thread to make sure the async thread created inside Kafka is also a daemon thread.
   */
  private <K, V> Producer<K, V> createProducer() throws ExecutionException, InterruptedException, TimeoutException {
    // Start it on demand. We need the broker service to discover Kafka brokers
    // It will be stopped by the running process on shutdown
    if (!brokerService.isRunning()) {
      brokerService.startAndWait();
    }

    // Try to find the broker list first
    final SettableFuture<String> brokers = SettableFuture.create();
    BrokerService.BrokerChangeListener listener = new BrokerService.BrokerChangeListener() {
      @Override
      public void changed(BrokerService brokerService) {
        String brokerList = brokerService.getBrokerList();
        if (!brokerList.isEmpty()) {
          brokers.set(brokerList);
        }
      }
    };
    Cancellable cancellable = brokerService.addChangeListener(listener, Threads.SAME_THREAD_EXECUTOR);
    try {
      // Invoke it once manually. If there broker list is already discovered, it will be set to the future;
      // otherwise the callback will be triggered when the broker list is ready.
      listener.changed(brokerService);
      producerProperties.setProperty("metadata.broker.list",
                                     brokers.get(PRODUCER_CREATION_TIMEOUT_SECS, TimeUnit.SECONDS));
    } finally {
      cancellable.cancel();
    }

    // If we get the broker list, then create a new daemon thread to instantiate the producer
    ExecutorService executor = Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("create-producer"));
    try {
      return executor.submit(new Callable<Producer<K, V>>() {
        @Override
        public Producer<K, V> call() throws Exception {
          return new Producer<>(new ProducerConfig(producerProperties));
        }
      }).get();
    } finally {
      executor.shutdownNow();
    }
  }
}
