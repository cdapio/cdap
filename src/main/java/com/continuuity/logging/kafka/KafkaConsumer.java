package com.continuuity.logging.kafka;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.StatusCode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.continuuity.common.logging.LoggingConfiguration.KafkaHost;
import static kafka.api.OffsetRequest.CurrentVersion;

/**
 * Kafka consumer that listens on a topic/partition and retrieves messages.
 */
public final class KafkaConsumer implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

  private static final int MAX_KAFKA_FETCH_RETRIES = 5;

  private final List<KafkaHost> replicaBrokers;
  private final String topic;
  private final int partition;
  private final int fetchTimeoutMs;
  private final String clientName;

  private String leaderHostName;
  private int leaderPort;

  private SimpleConsumer consumer;

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
    clientName = String.format("%s_%s_%d", getClass().getName(), topic, partition);
  }

  /**
   * Fetches Kafka messages from an offset.
   * @param offset message offset to start.
   * @param messageList a list to add the fetched messages into. The list will not be truncated by the method.
   * @throws OperationException
   */
  public int fetchMessages(long offset, int sizeBytes, List<KafkaMessage> messageList) throws OperationException {
    ByteBufferMessageSet messageSet = fetchMessageSet(offset, sizeBytes);
    for (MessageAndOffset msg : messageSet) {
      byte[] bytes = new byte[msg.message().payload().limit()];
      msg.message().payload().get(bytes);
      messageList.add(new KafkaMessage(ByteBuffer.wrap(bytes), msg.offset()));
    }
    return messageSet.validBytes();
  }

  /**
   * Fetch the earliest or latest available Kafka message offset.
   * @param offset offset to fetch.
   * @return Kafka message offset.
   */
  public long fetchOffset(Offset offset) throws OperationException {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = Maps.newHashMap();
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(offset.getValue(), 1));
    OffsetRequest request = new OffsetRequest(requestInfo, CurrentVersion(), clientName);

    if (consumer == null) {
      initialize();
    }
    OffsetResponse response = consumer.getOffsetsBefore(request);

    if (response.hasError()) {
      throw new OperationException(
        StatusCode.INTERNAL_ERROR,
        String.format("Error fetching offset data from broker %s:%d for topic %s, partition %d. Error code: %d",
                      leaderHostName, leaderPort, topic, partition, response.errorCode(topic, partition)));
    }
    long[] offsets = response.offsets(topic, partition);
    if (offsets.length == 0) {
      LOG.warn(
        String.format("Got zero offsets in offset response for time %s from broker %s:%d for topic %s, partiton %d",
                      offset, leaderHostName, leaderPort, topic, partition));
      return 0;
    }
    return offsets[0];
  }

  @Override
  public void close() throws IOException {
    if (consumer != null) {
      consumer.close();
    }
  }

  private ByteBufferMessageSet fetchMessageSet(long fetchOffset, int sizeBytes) throws OperationException {
    short errorCode = 0;
    for (int i = 0; i < MAX_KAFKA_FETCH_RETRIES; ++i) {
      if (consumer == null) {
          initialize();
      }
      if (fetchOffset >= 0) {
        FetchRequest req = new FetchRequestBuilder()
          .clientId(clientName)
          .addFetch(topic, partition, fetchOffset, sizeBytes)
          .maxWait(fetchTimeoutMs)
          .build();
        FetchResponse fetchResponse = consumer.fetch(req);

        if (fetchResponse.hasError()) {
          // Something went wrong!
          errorCode = fetchResponse.errorCode(topic, partition);
          LOG.warn(
            String.format("Error fetching data from broker %s:%d for topic %s, partition %d. Error code: %d",
                          leaderHostName, leaderPort, topic, partition, errorCode));
          if (errorCode == ErrorMapping.OffsetOutOfRangeCode())  {
            // We asked for an invalid offset. For simple case ask for the last element to reset
            throw new OperationException(
              StatusCode.INTERNAL_ERROR,
              String.format("Requested offset %d is out of range for topic %s partition %d",
                            fetchOffset, topic, partition));
          }
          consumer.close();
          consumer = null;
          findNewLeader();
          continue;
        }

        return fetchResponse.messageSet(topic, partition);
      }
    }
    throw new OperationException(
      StatusCode.INTERNAL_ERROR,
      String.format("Error fetching data from broker %s:%d for topic %s, partition %d. Error code: %d",
                    leaderHostName, leaderPort, topic, partition, errorCode));
  }

  private void initialize() throws OperationException {
    if (leaderHostName == null) {
      PartitionMetadata metadata = findLeader(replicaBrokers, topic, partition);
      if (metadata == null) {
        String message = String.format("Can't find metadata for topic %s and partition %d with brokers %s.",
                                       topic, partition, replicaBrokers.toString());
        LOG.warn(message);
        throw new OperationException(StatusCode.INTERNAL_ERROR, message);
      }
      if (metadata.leader() == null) {
        String message = String.format("Can't find leader for topic %s and partition %d with brokers %s.",
                                       topic, partition, replicaBrokers.toString());
        LOG.warn(message);
        throw new OperationException(StatusCode.INTERNAL_ERROR, message);
      }
      leaderHostName = metadata.leader().host();
      leaderPort = metadata.leader().port();
    }
    consumer = new SimpleConsumer(leaderHostName, leaderPort, 100000, 64 * 1024, clientName);
  }

  private void findNewLeader() throws OperationException {
    for (int i = 0; i < 3; i++) {
      PartitionMetadata metadata = findLeader(replicaBrokers, topic, partition);
      if (metadata == null || metadata.leader() == null) {
        LOG.warn(String.format("Not able to fetch leader information for topic %s, partition %d", topic, partition));
      } else if (leaderHostName.equalsIgnoreCase(metadata.leader().host()) &&
        leaderPort == metadata.leader().port() && i == 0) {
        // first time through if the leader hasn't changed give ZooKeeper a second to recover
        // second time, assume the broker did recover before failover, or it was a non-Broker issue
        //
      } else {
        leaderHostName = metadata.leader().host();
        leaderPort = metadata.leader().port();
        saveReplicaBrokers(metadata);
        return;
      }
      try {
        int sleepMs = 1000;
        LOG.info(String.format("Sleeping for %d ms", sleepMs));
        Thread.sleep(sleepMs);
      } catch (InterruptedException e) {
        LOG.warn("Caught InterruptedException while sleeping", e);
      }
    }
    throw new OperationException(StatusCode.INTERNAL_ERROR,
                                 String.format(
                                   "Unable to find new leader after broker %s:%d failure for topic %s, partition %d.",
                                   leaderHostName, leaderPort, topic, partition));
  }

  private void saveReplicaBrokers(PartitionMetadata partitionMetadata) {
    if (partitionMetadata != null) {
      replicaBrokers.clear();
      for (kafka.cluster.Broker replica : partitionMetadata.replicas()) {
        replicaBrokers.add(new KafkaHost(replica.host(), replica.port()));
      }
    }
  }

  private static PartitionMetadata findLeader(List<KafkaHost> replicaBrokers, String topic,
                                              int partition) throws OperationException {
    PartitionMetadata returnMetaData = null;
    for (KafkaHost broker : replicaBrokers) {
      SimpleConsumer consumer = null;
      try {
        consumer = new SimpleConsumer(broker.getHostname(), broker.getPort(), 100000, 64 * 1024, "leaderLookup");
        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        TopicMetadataResponse resp = consumer.send(req);

        List<TopicMetadata> metaData = resp.topicsMetadata();
        for (TopicMetadata item : metaData) {
          for (PartitionMetadata part : item.partitionsMetadata()) {
            if (part.partitionId() == partition) {
              returnMetaData = part;
              break;
            }
          }
        }
      } catch (Exception e) {
        LOG.error(
          String.format("Error commnicating with broker %s:%d to find leader for topic %s, partition %d",
                        broker.getHostname(), broker.getPort(), topic, partition), e);
      } finally {
        if (consumer != null) {
          consumer.close();
        }
      }
    }
    if (returnMetaData == null) {
      throw new OperationException(StatusCode.INTERNAL_ERROR,
                                   String.format("Could not find leader for topic %s, partition %d",
                                                 topic, partition));
    }
    return returnMetaData;
  }
}
