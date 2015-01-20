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

package co.cask.cdap.metrics.process;

import co.cask.cdap.data2.OperationException;
import co.cask.cdap.metrics.MetricsConstants.ConfigKeys;
import co.cask.cdap.metrics.data.MetricsTableFactory;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
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
import java.util.concurrent.TimeUnit;

/**
 * Process metrics by consuming metrics being published to kafka.
 */
public final class KafkaMetricsProcessorService extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsProcessorService.class);

  private final KafkaClientService kafkaClient;
  private final MessageCallbackFactory callbackFactory;
  private final String topicPrefix;
  private final Set<Integer> partitions;
  private Cancellable unsubscribe;
  private final MetricsTableFactory metricsTableFactory;

  private volatile boolean stopping = false;

  private KafkaConsumerMetaTable metaTable;

  @Inject
  public KafkaMetricsProcessorService(KafkaClientService kafkaClient,
                                      MetricsTableFactory metricsTableFactory,
                                      MessageCallbackFactory callbackFactory,
                                      @Named(ConfigKeys.KAFKA_TOPIC_PREFIX) String topicPrefix,
                                      @Assisted Set<Integer> partitions) {
    this.kafkaClient = kafkaClient;
    this.callbackFactory = callbackFactory;
    this.topicPrefix = topicPrefix;
    this.partitions = partitions;
    this.metricsTableFactory = metricsTableFactory;
  }

  @Override
  protected String getServiceName() {
    return this.getClass().getSimpleName();
  }

  @Override
  protected void run() {
    LOG.info("Starting Metrics Processing for partitions {}.", partitions);
    subscribe();
    LOG.info("Metrics Processing Service started for partitions {}.", partitions);

    while (isRunning()) {
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        // It's triggered by stop
        Thread.currentThread().interrupt();
        continue;
      }
    }
  }

  @Override
  protected void triggerShutdown() {
    LOG.info("Shutdown is triggered.");
    stopping = true;
    super.triggerShutdown();
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

  private KafkaConsumerMetaTable getMetaTable() {
    while (metaTable == null) {
      if (stopping) {
        LOG.info("We are shutting down, giving up on acquiring KafkaConsumerMetaTable.");
        break;
      }
      try {
        metaTable = metricsTableFactory.createKafkaConsumerMeta();
      } catch (Exception e) {
        LOG.warn("Cannot access kafka consumer metaTable, will retry in 1 sec.");
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    return metaTable;
  }

  private void subscribe() {
    List<Cancellable> cancels = Lists.newArrayList();
    // Assuming there is only one process that pulling in all metrics.
    KafkaConsumer.Preparer preparer = kafkaClient.getConsumer().prepare();

    String topic = topicPrefix;
    for (int i : partitions) {
      long offset = getOffset(topic, i);
      if (offset >= 0) {
        preparer.add(topic, i, offset);
      } else {
        preparer.addFromBeginning(topic, i);
      }
    }

    unsubscribe = preparer.consume(callbackFactory.create(getMetaTable()));
    LOG.info("Consumer created for topic {}, partitions {}", topic, partitions);
  }

  private long getOffset(String topic, int partition) {
    LOG.info("Retrieve offset for topic: {}, partition: {}", topic, partition);
    try {
      KafkaConsumerMetaTable metaTable = getMetaTable();
      if (metaTable == null) {
        LOG.info("Could not get KafkaConsumerMetaTable, seems like we are being shut down");
        return -1L;
      }
      long offset = metaTable.get(new TopicPartition(topic, partition));
      LOG.info("Offset for topic: {}, partition: {} is {}", topic, partition, offset);
      return offset;
    } catch (OperationException e) {
      LOG.error("Failed to get offset from meta table. Defaulting to beginning. {}", e.getMessage(), e);
    }
    return -1L;
  }
}
