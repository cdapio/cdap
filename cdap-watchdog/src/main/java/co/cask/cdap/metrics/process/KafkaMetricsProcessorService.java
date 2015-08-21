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

import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
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

import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

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
  private final MetricDatasetFactory metricDatasetFactory;

  @Nullable
  private MetricsContext metricsContext;

  private volatile boolean stopping = false;

  private KafkaConsumerMetaTable metaTable;

  @Inject
  public KafkaMetricsProcessorService(KafkaClientService kafkaClient,
                                      MetricDatasetFactory metricDatasetFactory,
                                      MessageCallbackFactory callbackFactory,
                                      @Named(Constants.Metrics.KAFKA_TOPIC_PREFIX) String topicPrefix,
                                      @Assisted Set<Integer> partitions) {
    this.kafkaClient = kafkaClient;
    this.callbackFactory = callbackFactory;
    this.topicPrefix = topicPrefix;
    this.partitions = partitions;
    this.metricDatasetFactory = metricDatasetFactory;
  }

  public void setMetricsContext(MetricsContext metricsContext) {
    this.metricsContext = metricsContext;
  }

  @Override
  protected String getServiceName() {
    return this.getClass().getSimpleName();
  }

  @Override
  protected void run() {
    LOG.info("Starting Metrics Processing for partitions {}.", partitions);
    if (!subscribe()) {
      return;
    }
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
        metaTable = metricDatasetFactory.createKafkaConsumerMeta();
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

  private boolean subscribe() {
    // Assuming there is only one process that pulling in all metrics.
    KafkaConsumer.Preparer preparer = kafkaClient.getConsumer().prepare();

    String topic = topicPrefix;

    for (int partition : partitions) {
      long offset;
      try {
        LOG.info("Retrieve offset for topic: {}, partition: {}", topic, partition);
        KafkaConsumerMetaTable metaTable = getMetaTable();
        if (metaTable == null) {
          LOG.info("Could not get KafkaConsumerMetaTable, seems like we are being shut down");
          return false;
        }
        offset = metaTable.get(new TopicPartition(topic, partition));
        LOG.info("Offset for topic: {}, partition: {} is {}", topic, partition, offset);
      } catch (Exception e) {
        LOG.info("Failed to get KafkaConsumerMetaTable, shutting down");
        return false;
      }

      if (offset >= 0) {
        preparer.add(topic, partition, offset);
      } else {
        preparer.addFromBeginning(topic, partition);
      }
    }

    unsubscribe = preparer.consume(callbackFactory.create(getMetaTable(), metricsContext));
    LOG.info("Consumer created for topic {}, partitions {}", topic, partitions);
    return true;
  }
}
