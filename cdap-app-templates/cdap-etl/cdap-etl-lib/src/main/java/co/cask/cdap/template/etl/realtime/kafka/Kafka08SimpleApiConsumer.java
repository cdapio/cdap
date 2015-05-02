/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.template.etl.realtime.kafka;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.realtime.RealtimeContext;
import co.cask.cdap.template.etl.realtime.source.KafkaSource;
import co.cask.cdap.template.etl.realtime.source.KafkaSource.KafkaPluginConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
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
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.twill.kafka.client.BrokerInfo;
import org.apache.twill.kafka.client.BrokerService;
import org.apache.twill.kafka.client.TopicPartition;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * <p>
 *  The {@link KafkaSimpleApiConsumer} compatible mode for {@code Kafka} 0.8.x releases.
 *
 *  The class expect several runtime arguments:
 *  -) kafka.zookeeper - the location of Kafka Zookeeper
 *  -) kafka.brokers - comma separate list of Kafka brokers
 *  -) kafka.partitions - the number of partitions of the topic
 *  -) kafka.topic - the Kafka topic to get messages for
 *  -) kafka.default.offset - the default offset for the partition
 * </p>
 */
public class Kafka08SimpleApiConsumer extends KafkaSimpleApiConsumer<String, ByteBuffer, Long>  {
  private static final Logger LOG = LoggerFactory.getLogger(Kafka08SimpleApiConsumer.class);

  private ZKClientService zkClient;
  private BrokerService brokerService;
  private Cache<TopicPartition, SimpleConsumer> kafkaConsumers;

  public Kafka08SimpleApiConsumer(KafkaSource kafkaSource) {
    super(kafkaSource);
  }

  @Override
  protected void configureKafka(KafkaConfigurer configurer) {
    KafkaPluginConfig pluginConfig = kafkaSource.getConfig();
    Preconditions.checkNotNull(pluginConfig, "Could not have Kafka source plugin config to be null.");

    String zk = pluginConfig.getZkConnect();
    String brokers = pluginConfig.getKafkaBrokers();
    Preconditions.checkState(zk != null || brokers != null);

    if (zk != null) {
      configurer.setZooKeeper(zk);
    } else {
      configurer.setBrokers(brokers);
    }

    setupTopicPartitions(configurer, pluginConfig);
  }

  private void setupTopicPartitions(KafkaConsumerConfigurer configurer, KafkaPluginConfig pluginConfig) {
    int partitions = pluginConfig.getPartitions();
    int instanceId = getContext().getInstanceId();
    int instances = getContext().getInstanceCount();
    String kafkaTopic = pluginConfig.getTopic();
    for (int i = 0; i < partitions; i++) {
      if ((i % instances) == instanceId) {
        configurer.addTopicPartition(kafkaTopic, i);
      }
    }
  }

  @Override
  protected Iterator<KafkaMessage<Long>> readMessages(KafkaConsumerInfo<Long> consumerInfo) {
    final TopicPartition topicPartition = consumerInfo.getTopicPartition();
    String topic = topicPartition.getTopic();
    int partition = topicPartition.getPartition();

    // Fetch message from Kafka.
    final SimpleConsumer consumer = getConsumer(consumerInfo);
    if (consumer == null) {
      return Iterators.emptyIterator();
    }

    long readOffset = consumerInfo.getReadOffset();
    if (readOffset < 0) {
      readOffset = getReadOffset(consumer, topic, partition, readOffset);
      consumerInfo.setReadOffset(readOffset);
    }

    FetchRequest fetchRequest = new FetchRequestBuilder()
      .clientId(consumer.clientId())
      .addFetch(topic, partition, readOffset, consumerInfo.getFetchSize())
      .build();
    FetchResponse response = consumer.fetch(fetchRequest);

    // Fetch failed
    if (response.hasError()) {
      handleFetchError(consumerInfo, consumer, readOffset, response.errorCode(topic, partition));
      return Iterators.emptyIterator();
    }

    // Returns an Iterator of message
    final long fetchReadOffset = readOffset;
    final Iterator<MessageAndOffset> messages = response.messageSet(topic, partition).iterator();
    return new AbstractIterator<KafkaMessage<Long>>() {
      @Override
      protected KafkaMessage<Long> computeNext() {
        while (messages.hasNext()) {
          MessageAndOffset messageAndOffset = messages.next();
          if (messageAndOffset.offset() < fetchReadOffset) {
            // Older message read (which is valid in Kafka), skip it.
            continue;
          }

          // Lets get the Kafka message based on last offset
          Message message = messageAndOffset.message();
          return new KafkaMessage<Long>(topicPartition, messageAndOffset.nextOffset(), message.key(),
                                        message.payload());
        }
        return endOfData();
      }
    };
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);

    // Setting up ZK for 08 version of Apache Kafka
    String kafkaZKConnect = getKafkaConfig().getZookeeper();
    if (kafkaZKConnect != null) {
      zkClient = ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(ZKClientService.Builder.of(kafkaZKConnect).build(),
                                   RetryStrategies.fixDelay(2, TimeUnit.SECONDS))
        ));
      zkClient.startAndWait();

      brokerService = createBrokerService(zkClient);
      brokerService.startAndWait();
    }

    kafkaConsumers = CacheBuilder.newBuilder()
      .concurrencyLevel(1)
      .expireAfterAccess(60, TimeUnit.SECONDS)
      .removalListener(consumerCacheRemovalListener())
      .build();
  }

  /**
   * Creates a {@link RemovalListener} to close {@link SimpleConsumer} when it is evicted from the consumer cache.
   */
  private RemovalListener<TopicPartition, SimpleConsumer> consumerCacheRemovalListener() {
    return new RemovalListener<TopicPartition, SimpleConsumer>() {
      @Override
      public void onRemoval(RemovalNotification<TopicPartition, SimpleConsumer> notification) {
        SimpleConsumer consumer = notification.getValue();
        if (consumer == null) {
          return;
        }
        try {
          consumer.close();
        } catch (Throwable t) {
          LOG.error("Exception when closing Kafka consumer.", t);
        }
      }
    };
  }

  @Override
  public void destroy() {
    super.destroy();
    if (kafkaConsumers != null) {
      kafkaConsumers.invalidateAll();
      kafkaConsumers.cleanUp();
    }

    if (brokerService != null) {
      stopService(brokerService);
    }
    if (zkClient != null) {
      stopService(zkClient);
    }
  }

  @Override
  protected void processMessage(String key, ByteBuffer payload, Emitter<StructuredRecord> emitter) {
    StructuredRecord structuredRecord = byteBufferToStructuredRecord(key, payload);
    emitter.emit(structuredRecord);
  }

  @Override
  protected void processMessage(ByteBuffer payload , Emitter<StructuredRecord> emitter) {
    StructuredRecord structuredRecord = byteBufferToStructuredRecord(null, payload);
    emitter.emit(structuredRecord);
  }

  @Override
  protected Long getBeginOffset(TopicPartition topicPartition) {
    Map<String, byte[]> offsetStore  = getOffsetStore();
    if (offsetStore == null) {
      return getDefaultOffset(topicPartition);
    }

    // The value is simply a 8-bytes long representing the offset
    byte[] value = offsetStore.get(getStoreKey(topicPartition));
    if (value == null || value.length != Bytes.SIZEOF_LONG) {
      return getDefaultOffset(topicPartition);
    }
    return Bytes.toLong(value);
  }

  @Override
  protected void saveReadOffsets(Map<TopicPartition, Long> offsets) {
    Map<String, byte[]> offsetStore  = getOffsetStore();
    if (offsetStore == null) {
      return;
    }

    for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
      offsetStore.put(getStoreKey(entry.getKey()), Bytes.toBytes(entry.getValue()));
    }
  }

  @Override
  protected long getDefaultOffset(TopicPartition topicPartition) {
    Long argValue = kafkaSource.getConfig().getDefaultOffset();
    if (argValue != null) {
      return argValue.longValue();
    } else {
      return kafka.api.OffsetRequest.EarliestTime();
    }
  }

  private long getReadOffset(SimpleConsumer consumer, String topic, int partition, long time) {
    OffsetRequest offsetRequest = new OffsetRequest(
      ImmutableMap.of(new TopicAndPartition(topic, partition), new PartitionOffsetRequestInfo(time, 1)),
      kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());

    OffsetResponse response = consumer.getOffsetsBefore(offsetRequest);
    if (response.hasError()) {
      LOG.error("Failed to fetch offset from broker {}:{} for topic-partition {}-{} with error code {}",
                consumer.host(), consumer.port(), topic, partition, response.errorCode(topic, partition));
      return 0L;
    }
    return response.offsets(topic, partition)[0];
  }

  /**
   * Returns a {@link SimpleConsumer} for the given consumer info or {@code null} if no leader broker is currently
   * available.
   */
  @Nullable
  private SimpleConsumer getConsumer(KafkaConsumerInfo<Long> consumerInfo) {
    TopicPartition topicPartition = consumerInfo.getTopicPartition();
    SimpleConsumer consumer = kafkaConsumers.getIfPresent(topicPartition);
    if (consumer != null) {
      return consumer;
    }

    InetSocketAddress leader = getLeader(topicPartition.getTopic(), topicPartition.getPartition());
    if (leader == null) {
      return null;
    }
    String consumerName = String.format("%s-%d-kafka-consumer", getName(), getContext().getInstanceId());
    consumer = new SimpleConsumer(leader.getHostName(), leader.getPort(),
                                  SO_TIMEOUT, consumerInfo.getFetchSize(), consumerName);
    kafkaConsumers.put(topicPartition, consumer);

    return consumer;
  }

  /**
   * Handles fetch failure.
   *
   * @param consumerInfo information on what and how to consume
   * @param consumer consumer to talk to Kafka
   * @param readOffset the beginning read offset
   * @param errorCode error code for the fetch.
   */
  private void handleFetchError(KafkaConsumerInfo<Long> consumerInfo,
                                SimpleConsumer consumer, long readOffset, short errorCode) {
    TopicPartition topicPartition = consumerInfo.getTopicPartition();
    String topic = topicPartition.getTopic();
    int partition = topicPartition.getPartition();

    LOG.error("Failed to fetch from broker {}:{} for topic-partition {}-{} with error code {}",
              consumer.host(), consumer.port(), topic, partition, errorCode);
    if (errorCode == ErrorMapping.OffsetOutOfRangeCode()) {
      // Get the earliest offset
      long earliest = getReadOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime());
      // If the current read offset is smaller than the earliest one, use the earliest offset in next fetch
      if (readOffset < earliest) {
        consumerInfo.setReadOffset(earliest);
      } else {
        // Otherwise the read offset must be larger than the latest (otherwise it won't have the out of range error)
        consumerInfo.setReadOffset(getReadOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime()));
      }
    } else {
      // For other type of error, invalid it from cache so that a new one will be created in next iteration
      kafkaConsumers.invalidate(topicPartition);
    }
  }

  /**
   * Creates a Kafka {@link BrokerService}.
   */
  private BrokerService createBrokerService(ZKClient zkClient) throws Exception {
    Class<? extends BrokerService> brokerClass = loadBrokerServiceClass(BrokerService.class.getClassLoader());
    Constructor<? extends BrokerService> constructor = brokerClass.getDeclaredConstructor(ZKClient.class);
    constructor.setAccessible(true);
    return constructor.newInstance(zkClient);
  }

  /**
   * Loads the class that implements {@link BrokerService}.
   */
  @SuppressWarnings("unchecked")
  private <T> Class<T> loadBrokerServiceClass(ClassLoader classLoader) throws ClassNotFoundException {
    // TODO: Hacky way to construct ZKBrokerService from Twill as it's a protected class
    // TODO: Need Twill update to resolve this hack: https://github.com/apache/incubator-twill/pull/31
    return (Class<T>) classLoader.loadClass("org.apache.twill.internal.kafka.client.ZKBrokerService");
  }

  /**
   * Gets the address of the leader broker for the given topic and partition
   *
   * @return the address for the leader broker or {@code null} if no leader is currently available.
   */
  @Nullable
  private InetSocketAddress getLeader(String topic, int partition) {
    // If BrokerService is available, it will use information from ZooKeeper
    if (brokerService != null) {
      BrokerInfo brokerInfo = brokerService.getLeader(topic, partition);
      return (brokerInfo == null) ? null : new InetSocketAddress(brokerInfo.getHost(), brokerInfo.getPort());
    }

    // Otherwise use broker list to discover leader
    return findLeader(getKafkaConfig().getBrokers(), topic, partition);
  }

  /**
   * Finds the leader broker address for the given topic partition.
   *
   * @return the address for the leader broker or {@code null} if no leader is currently available.
   */
  @Nullable
  private InetSocketAddress findLeader(String brokers, String topic, int partition) {
    // Splits the broker list of format "host:port,host:port" into a map<host, port>
    Map<String, String> brokerMap = Splitter.on(',').withKeyValueSeparator(":").split(brokers);

    // Go through the broker list and try to find a leader for the given topic partition.
    for (Map.Entry<String, String> broker : brokerMap.entrySet()) {
      try {
        SimpleConsumer consumer = new SimpleConsumer(broker.getKey(), Integer.parseInt(broker.getValue()), SO_TIMEOUT,
                                                     KafkaConsumerConfigurer.DEFAULT_FETCH_SIZE, "leaderLookup");
        try {
          TopicMetadataRequest request = new TopicMetadataRequest(ImmutableList.of(topic));
          TopicMetadataResponse response = consumer.send(request);
          for (TopicMetadata topicData : response.topicsMetadata()) {
            for (PartitionMetadata partitionData : topicData.partitionsMetadata()) {
              if (partitionData.partitionId() == partition) {
                Broker leader = partitionData.leader();
                return new InetSocketAddress(leader.host(), leader.port());
              }
            }
          }

        } finally {
          consumer.close();
        }
      } catch (Exception e) {
        LOG.error("Failed to communicate with broker {}:{} for leader lookup for topic-partition {}-{}",
                  broker.getKey(), broker.getValue(), topic, partition, e);
      }
    }
    return null;
  }
}
