/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.kafka;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.OffsetOutOfRangeException;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.continuuity.logging.LoggingConfiguration.KafkaHost;
import static kafka.api.OffsetRequest.CurrentVersion;

/**
 * Kafka consumer that listens on a topic/partition and retrieves messages. This class is thread-safe.
 */
public final class KafkaConsumer implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

  private static final int MAX_KAFKA_FETCH_RETRIES = 5;
  public static final int BUFFER_SIZE_BYTES = 1024 * 1024;
  public static final int TIMEOUT_MS = 3000;

  private final List<KafkaHost> replicaBrokers;
  private final String topic;
  private final int partition;
  private final int fetchTimeoutMs;
  private final String clientName;

  // Simple consumer is thread safe
  private volatile SimpleConsumer consumer;

  /**
   * Represents the Kafka offsets that can be fetched by KafkaConsumer. Only earliest and latest offset are supported.
   */
  public enum Offset {
    EARLIEST(-2), LATEST(-1);

    private int value;

    private Offset(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  /**
   * Creates a KafkaConsumer with initial set of seed brokers, topic and partition.
   * @param seedBrokers list of seed brokers
   * @param topic Kafka topic to subscribe to
   * @param partition topic partition to subscribe to
   * @param fetchTimeoutMs timeout for a Kafka fetch call
   */
  public KafkaConsumer(List<KafkaHost> seedBrokers, String topic, int partition,
                       int fetchTimeoutMs) {
    this.replicaBrokers = Lists.newArrayList(seedBrokers);
    this.topic = topic;
    this.partition = partition;
    this.fetchTimeoutMs = fetchTimeoutMs;
    this.clientName = String.format("%s_%s_%d", getClass().getName(), topic, partition);
  }

  /**
   * Fetches Kafka messages from an offset.
   * @param offset message offset to start.
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
   * Fetch the earliest or latest available Kafka message offset.
   * @param offset offset to fetch.
   * @return Kafka message offset.
   */
  public long fetchOffset(Offset offset) {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = Maps.newHashMap();
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(offset.getValue(), 1));
    OffsetRequest request = new OffsetRequest(requestInfo, CurrentVersion(), clientName);

    if (consumer == null) {
      findLeader();
    }
    OffsetResponse response = consumer.getOffsetsBefore(request);

    if (response.hasError()) {
      // Try once more
      findLeader();
      response = consumer.getOffsetsBefore(request);

      if (response.hasError()) {
        closeConsumer();
        String message = String.format(
          "Error fetching offset data from broker %s:%d for topic %s, partition %d. Error code: %d",
          consumer.host(), consumer.port(), topic, partition, response.errorCode(topic, partition));
        LOG.error(message);
        throw new RuntimeException(message);
      }
    }
    long[] offsets = response.offsets(topic, partition);
    if (offsets.length == 0) {
      closeConsumer();
      String message =
        String.format("Got zero offsets in offset response for time %s from broker %s:%d for topic %s, partiton %d",
                      offset, consumer.host(), consumer.port(), topic, partition);
      LOG.error(message);
      throw new RuntimeException(message);
    }
    return offsets[0];
  }

  public boolean isLeaderPresent() {
    PartitionMetadata metadata = fetchPartitonMetadata();
    return !(metadata == null || metadata.errorCode() != ErrorMapping.NoError());
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

    short errorCode = 0;
    for (int i = 0; i < MAX_KAFKA_FETCH_RETRIES; ++i) {
      if (consumer == null) {
          findLeader();
      }

      FetchRequest req = new FetchRequestBuilder()
        .clientId(clientName)
        .addFetch(topic, partition, fetchOffset, BUFFER_SIZE_BYTES)
        .maxWait(fetchTimeoutMs)
        .build();
      FetchResponse fetchResponse = consumer.fetch(req);

      if (fetchResponse.hasError()) {
        errorCode = fetchResponse.errorCode(topic, partition);
        LOG.warn(
          String.format("Error fetching data from broker %s:%d for topic %s, partition %d. Error code: %d",
                        consumer.host(), consumer.port(), topic, partition, errorCode));
        if (errorCode == ErrorMapping.OffsetOutOfRangeCode())  {
          throw new OffsetOutOfRangeException(
            String.format("Requested offset %d is out of range for topic %s partition %d",
                          fetchOffset, topic, partition));
        }
        findLeader();
        continue;
      }

      return fetchResponse.messageSet(topic, partition);
    }
    String message = String.format("Error fetching data from broker %s:%d for topic %s, partition %d. Error code: %d",
                                   consumer.host(), consumer.port(), topic, partition, errorCode);
    LOG.error(message);
    throw new RuntimeException(message);
  }

  private void saveReplicaBrokers(PartitionMetadata partitionMetadata) {
    if (partitionMetadata != null) {
      replicaBrokers.clear();
      for (kafka.cluster.Broker replica : partitionMetadata.replicas()) {
        replicaBrokers.add(new KafkaHost(replica.host(), replica.port()));
      }
    }
  }

  private PartitionMetadata fetchPartitonMetadata() {
    for (KafkaHost broker : replicaBrokers) {
      SimpleConsumer consumer = new SimpleConsumer(broker.getHostname(), broker.getPort(), TIMEOUT_MS,
                                                   BUFFER_SIZE_BYTES, clientName);
      try {
        List<String> topics = ImmutableList.of(topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        TopicMetadataResponse resp = consumer.send(req);

        List<TopicMetadata> topicMetadataList = resp.topicsMetadata();
        for (TopicMetadata item : topicMetadataList) {
          for (PartitionMetadata part : item.partitionsMetadata()) {
            if (part.partitionId() == partition) {
              return part;
            }
          }
        }
      } finally {
        consumer.close();
      }
    }
    return null;
  }

  private void findLeader() {
    closeConsumer();

    PartitionMetadata metadata = fetchPartitonMetadata();
    if (metadata == null) {
      String message = String.format("Could not find leader for topic %s, partition %d",
                                     topic, partition);
      LOG.error(message);
      throw new RuntimeException(message);
    }

    if (metadata.leader() == null) {
      LOG.warn("Can't find leader for topic {} and partition {} with brokers {}.",
               topic, partition, replicaBrokers, ErrorMapping.exceptionFor(metadata.errorCode()));
      throw new RuntimeException(ErrorMapping.exceptionFor(metadata.errorCode()));
    }
    consumer = new SimpleConsumer(metadata.leader().host(), metadata.leader().port(), TIMEOUT_MS, BUFFER_SIZE_BYTES,
                                  clientName);
    saveReplicaBrokers(metadata);
  }
}
