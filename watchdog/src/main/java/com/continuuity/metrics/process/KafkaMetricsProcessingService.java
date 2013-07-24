package com.continuuity.metrics.process;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.kafka.client.KafkaClientService;
import com.continuuity.kafka.client.KafkaConsumer;
import com.continuuity.kafka.client.TopicPartition;
import com.continuuity.metrics.MetricsConstants.ConfigKeys;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.continuuity.weave.common.Cancellable;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Service for processing metrics by consuming metrics being published to kafka.
 */
public final class KafkaMetricsProcessingService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsProcessingService.class);

  private final KafkaClientService kafkaClient;
  private final MetricsTableFactory tableFactory;
  private final MessageCallbackFactory callbackFactory;
  private final String topicPrefix;
  private final int partitionSize;
  private final List<Cancellable> unsubscribes;
  private KafkaConsumerMetaTable metaTable;

  @Inject
  public KafkaMetricsProcessingService(KafkaClientService kafkaClient,
                                       MetricsTableFactory tableFactory,
                                       MessageCallbackFactory callbackFactory,
                                       @Named(ConfigKeys.KAFKA_TOPIC_PREFIX) String topicPrefix,
                                       @Named(ConfigKeys.KAFKA_PARTITION_SIZE) int partitionSize) {
    this.kafkaClient = kafkaClient;
    this.tableFactory = tableFactory;
    this.callbackFactory = callbackFactory;
    this.topicPrefix = topicPrefix;
    this.partitionSize = partitionSize;
    this.unsubscribes = Lists.newArrayList();
  }

  @Override
  protected void startUp() {
    LOG.info("Starting Metrics Processing Service.");
    metaTable = tableFactory.createKafkaConsumerMeta("default");
    subscribe();
    LOG.info("Metrics Processing Service started.");
  }

  @Override
  protected void shutDown() {
    LOG.info("Stopping Metrics Processing Service.");
    for (Cancellable cancel : unsubscribes) {
      cancel.cancel();
    }
    LOG.info("Metrics Processing Service stopped.");
  }

  private void subscribe() {
    LOG.info("Prepare to subscribe.");

    for (MetricsScope scope : MetricsScope.values()) {
      // Assuming there is only one process that pulling in all metrics.
      KafkaConsumer.Preparer preparer = kafkaClient.getConsumer().prepare();

      String topic = topicPrefix + "." + scope.name();
      for (int i = 0; i < partitionSize; i++) {
        long offset = getOffset(topic, i);
        if (offset >= 0) {
          preparer.add(topic, i, offset);
        } else {
          preparer.addFromBeginning(topic, i);
        }
      }

      unsubscribes.add(preparer.consume(callbackFactory.create(metaTable, scope)));
      LOG.info("Consumer created for topic {}, partition size {}", topic, partitionSize);
    }

    LOG.info("Subscription ready.");
  }

  private long getOffset(String topic, int partition) {
    LOG.info("Retrieve offset for topic: {}, partition: {}", topic, partition);
    try {
      long offset = metaTable.get(new TopicPartition(topic, partition));
      LOG.info("Offset for topic: {}, partition: {} is {}", topic, partition, offset);
      return offset;
    } catch (OperationException e) {
      LOG.error("Failed to get offset from meta table. Defaulting to beginning. {}", e.getMessage(), e);
    }
    return -1L;
  }
}
