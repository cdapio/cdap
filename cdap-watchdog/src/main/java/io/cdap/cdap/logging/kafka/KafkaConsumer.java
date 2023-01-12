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

package io.cdap.cdap.logging.kafka;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.common.ErrorMapping;
import kafka.common.OffsetOutOfRangeException;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.twill.kafka.client.BrokerInfo;
import org.apache.twill.kafka.client.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Kafka consumer that listens on a topic/partition and retrieves messages. This class is thread-safe.
 */
public final class KafkaConsumer implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

  private static final int MAX_KAFKA_FETCH_RETRIES = 5;
  private static final int BUFFER_SIZE_BYTES = 1024 * 1024;
  private static final int TIMEOUT_MS = 3000;

  private final BrokerService brokerService;
  private final String topic;
  private final int partition;
  private final int fetchTimeoutMs;
  private final String clientName;

  // KafkaConsumer is not thread safe
  private org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer;
  private org.apache.kafka.clients.KafkaClient client;

  /**
   * Creates a KafkaConsumer with initial set of seed brokers, topic and partition.
   *
   * @param brokerService  the {@link BrokerService} for finding Kafka brokers.
   * @param topic          Kafka topic to subscribe to
   * @param partition      topic partition to subscribe to
   * @param fetchTimeoutMs timeout for a Kafka fetch call
   */
  public KafkaConsumer(BrokerService brokerService, String topic, int partition, int fetchTimeoutMs) {
    this.brokerService = brokerService;
    this.topic = topic;
    this.partition = partition;
    this.fetchTimeoutMs = fetchTimeoutMs;
    this.clientName = String.format("%s_%s_%d", getClass().getName(), topic, partition);
  }

  /**
   * Fetches Kafka messages from an offset.
   *
   * @param offset   message offset to start.
   * @param callback callback to handle the messages fetched.
   * @return number of messages fetched.
   */
  public int fetchMessages(long offset, Callback callback) throws OffsetOutOfRangeException {
    ByteBufferMessageSet messageSet = fetchMessageSet(offset);
    int msgCount = 0;
    for (MessageAndOffset msg : messageSet) {
      ++msgCount;
      callback.handle(msg.offset(), msg.message().payload());
    }
    return msgCount;
  }

  /**
   * Fetches the earliest offset in Kafka.
   */
  public long fetchEarliestOffset() {
    return fetchOffsetBefore(kafka.api.OffsetRequest.EarliestTime());
  }

  /**
   * Fetches the latest offset in Kafka.
   */
  public long fetchLatestOffset() {
    return fetchOffsetBefore(kafka.api.OffsetRequest.LatestTime());
  }

  /**
   * Fetch offset before given time.
   *
   * @param timeMillis offset to fetch before timeMillis.
   * @return Kafka message offset
   */
  public long fetchOffsetBefore(long timeMillis) {
    TopicPartition topicAndPartition = new TopicPartition(topic, partition);
    org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = getConsumer();

    try {
      if (timeMillis == ListOffsetRequest.EARLIEST_TIMESTAMP) {
        Map<TopicPartition, Long> offsets = consumer.beginningOffsets(Arrays.asList(topicAndPartition));
        return offsets.get(topicAndPartition);
      }

      OffsetAndTimestamp offset = Retries.callWithRetries(
        new Retries.Callable<OffsetAndTimestamp, RetriableException>() {
          @Override
          public OffsetAndTimestamp call() throws RetriableException {
            Map<TopicPartition, Long> offsetRequest = Maps.newHashMap();
            offsetRequest.put(topicAndPartition, timeMillis);
            Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(offsetRequest);
            return offsets.get(topicAndPartition);
          }
        }, RetryStrategies.exponentialDelay(200, 10000, TimeUnit.MILLISECONDS));

      if (offset != null) {
        return offset.offset();
      }

    } catch (Exception e) {
      closeConsumer();
      throw new RuntimeException(String.format(
        "Error fetching offset data for topic %s, partition %d. Error: %s",
        topic, partition, e.getCause().getMessage()));
    }

    // No offsets returned for given time. If this is not a request for earliest offset, return earliest offset.
    // Otherwise throw exception.
    if (timeMillis != ListOffsetRequest.EARLIEST_TIMESTAMP) {
      return fetchOffsetBefore(ListOffsetRequest.EARLIEST_TIMESTAMP);
    }
    closeConsumer();
    throw new RuntimeException(String.format(
      "Got zero offsets in offset response for time %s for topic %s, partition %d",
      timeMillis, topic, partition));
  }

  private void closeConsumer() {
    if (consumer != null) {
      consumer.close();
      consumer = null;
    }
  }

  @Override
  public void close() throws IOException {
    closeConsumer();
  }

  private ByteBufferMessageSet fetchMessageSet(long fetchOffset) throws OffsetOutOfRangeException {
    Preconditions.checkArgument(fetchOffset >= 0, String.format("Illegal fetch offset %d", fetchOffset));

    int failureCount = 0;
    while (true) {
      org.apache.kafka.clients.KafkaClient<String, String> consumer = getConsumer();

      FetchRequest req = new FetchRequestBuilder()
        .clientId(clientName)
        .addFetch(topic, partition, fetchOffset, BUFFER_SIZE_BYTES)
        .maxWait(fetchTimeoutMs)
        .build();
      FetchResponse fetchResponse = consumer.poll(Duration.ofMillis(fetchTimeoutMs));

      if (!fetchResponse.hasError()) {
        return fetchResponse.messageSet(topic, partition);
      }
      short errorCode = fetchResponse.errorCode(topic, partition);

      if (++failureCount >= MAX_KAFKA_FETCH_RETRIES) {
        throw new RuntimeException(
          String.format("Error fetching data for topic %s, partition %d. Error code: %d", topic, partition, errorCode));
      }

      LOG.warn("Error fetching data for topic {}, partition {}. Error code: {}", topic, partition, errorCode);

      if (errorCode == ErrorMapping.OffsetOutOfRangeCode()) {
        throw new OffsetOutOfRangeException(String.format(
          "Requested offset %d is out of range for topic %s partition %d", fetchOffset, topic, partition));
      }
      closeConsumer();
    }
  }

  private org.apache.kafka.clients.consumer.KafkaConsumer<String, String> getConsumer() {
    if (consumer != null) {
      return consumer;
    }
    BrokerInfo leader = brokerService.getLeader(topic, partition);
    Properties clientProperties = new Properties();
    clientProperties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(TIMEOUT_MS));
    clientProperties.setProperty(ConsumerConfig.RECEIVE_BUFFER_CONFIG, Integer.toString(BUFFER_SIZE_BYTES));
    clientProperties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientName);
    clientProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, leader.getHost());
    consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(clientProperties);
    return consumer;
  }
}
