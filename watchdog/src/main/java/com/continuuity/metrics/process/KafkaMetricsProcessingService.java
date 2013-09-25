package com.continuuity.metrics.process;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.kafka.client.KafkaClientService;
import com.continuuity.kafka.client.KafkaConsumer;
import com.continuuity.kafka.client.TopicPartition;
import com.continuuity.metrics.MetricsConstants.ConfigKeys;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.continuuity.watchdog.election.LeaderChangeHandler;
import com.continuuity.watchdog.election.MultiLeaderElection;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * Service for processing metrics by consuming metrics being published to kafka.
 */
public final class KafkaMetricsProcessingService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsProcessingService.class);

  private final KafkaClientService kafkaClient;
  private final MetricsTableFactory tableFactory;
  private final MessageCallbackFactory callbackFactory;
  private final String topicPrefix;
  private final List<Cancellable> kafkaUnsubscribes;
  private final MultiLeaderElection multiElection;
  private KafkaConsumerMetaTable metaTable;

  @Inject
  public KafkaMetricsProcessingService(ZKClient zkClient,
                                       KafkaClientService kafkaClient,
                                       MetricsTableFactory tableFactory,
                                       MessageCallbackFactory callbackFactory,
                                       @Named(ConfigKeys.KAFKA_TOPIC_PREFIX) String topicPrefix,
                                       @Named(ConfigKeys.KAFKA_PARTITION_SIZE) int partitionSize) {
    this.kafkaClient = kafkaClient;
    this.tableFactory = tableFactory;
    this.callbackFactory = callbackFactory;
    this.topicPrefix = topicPrefix;
    this.kafkaUnsubscribes = Lists.newArrayList();

    this.multiElection = new MultiLeaderElection(zkClient, "metrics-processor",
                                                 partitionSize,
                                                 new LeaderChangeHandler() {
                                                   @Override
                                                   public void leaderChanged(Set<Integer> partitions) throws
                                                     Exception {
                                                     subscribe(partitions);
                                                   }
                                                 }
    );
  }

  @Override
  protected void startUp() {
    LOG.info("Starting Metrics Processing Service.");
    metaTable = tableFactory.createKafkaConsumerMeta("default");
    multiElection.startAndWait();
    LOG.info("Metrics Processing Service started.");
  }

  @Override
  protected void shutDown() {
    LOG.info("Stopping Metrics Processing Service.");

    // Cancel kafka subscriptions
    for (Cancellable cancel : kafkaUnsubscribes) {
      cancel.cancel();
    }

    // Cancel leader election
    multiElection.stopAndWait();

    LOG.info("Metrics Processing Service stopped.");
  }

  private void subscribe(Set<Integer> leaderPartitions) {
    // Don't subscribe when stopping
    if (state() == State.STOPPING) {
      LOG.info("Not subscribing when stopping!");
      return;
    }

    // Cancel any existing subscriptions
    for (Cancellable cancel : kafkaUnsubscribes) {
      cancel.cancel();
    }

    LOG.info("Prepare to subscribe.");

    for (MetricsScope scope : MetricsScope.values()) {
      // Assuming there is only one process that pulling in all metrics.
      KafkaConsumer.Preparer preparer = kafkaClient.getConsumer().prepare();

      String topic = topicPrefix + "." + scope.name();
      for (int i : leaderPartitions) {
        long offset = getOffset(topic, i);
        if (offset >= 0) {
          preparer.add(topic, i, offset);
        } else {
          preparer.addFromBeginning(topic, i);
        }
      }

      kafkaUnsubscribes.add(preparer.consume(callbackFactory.create(metaTable, scope)));
      LOG.info("Consumer created for topic {}, partitions {}", topic, leaderPartitions);
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
