package com.continuuity.metrics.process;

import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.data2.OperationException;
import com.continuuity.metrics.MetricsConstants.ConfigKeys;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.name.Named;
import org.apache.twill.common.Cancellable;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.apache.twill.kafka.client.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * Process metrics by consuming metrics being published to kafka.
 */
public final class KafkaMetricsProcessorService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsProcessorService.class);

  private final KafkaClientService kafkaClient;
  private final KafkaConsumerMetaTable metaTable;
  private final MessageCallbackFactory callbackFactory;
  private final String topicPrefix;
  private final Set<Integer> partitions;
  private Cancellable unsubscribe;

  @Inject
  public KafkaMetricsProcessorService(KafkaClientService kafkaClient,
                                      KafkaConsumerMetaTable metaTable,
                                      MessageCallbackFactory callbackFactory,
                                      @Named(ConfigKeys.KAFKA_TOPIC_PREFIX) String topicPrefix,
                                      @Assisted Set<Integer> partitions) {
    this.kafkaClient = kafkaClient;
    this.metaTable = metaTable;
    this.callbackFactory = callbackFactory;
    this.topicPrefix = topicPrefix;
    this.partitions = partitions;
  }

  @Override
  protected void startUp() {
    LOG.info("Starting Metrics Processing for partitions {}.", partitions);
    subscribe();
    LOG.info("Metrics Processing Service started for partition {}.", partitions);
  }

  @Override
  protected void shutDown() {
    LOG.info("Stopping Metrics Processing Service.");

    // Cancel kafka subscriptions
    if (unsubscribe != null) {
      unsubscribe.cancel();
    }
    LOG.info("Metrics Processing Service stopped.");
  }

  private void subscribe() {
    List<Cancellable> cancels = Lists.newArrayList();
    for (MetricsScope scope : MetricsScope.values()) {
      // Assuming there is only one process that pulling in all metrics.
      KafkaConsumer.Preparer preparer = kafkaClient.getConsumer().prepare();

      String topic = topicPrefix + "." + scope.name().toLowerCase();
      for (int i : partitions) {
        long offset = getOffset(topic, i);
        if (offset >= 0) {
          preparer.add(topic, i, offset);
        } else {
          preparer.addFromBeginning(topic, i);
        }
      }

      cancels.add(preparer.consume(callbackFactory.create(metaTable, scope)));
      LOG.info("Consumer created for topic {}, partitions {}", topic, partitions);
    }
    unsubscribe = createCancelAll(cancels);
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

  private Cancellable createCancelAll(final Iterable<? extends Cancellable> cancels) {
    return new Cancellable() {
      @Override
      public void cancel() {
        for (Cancellable cancel : cancels) {
          cancel.cancel();
        }
      }
    };
  }
}
