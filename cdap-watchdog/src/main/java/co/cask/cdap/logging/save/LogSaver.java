/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.logging.save;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.Id;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.name.Named;
import org.apache.twill.common.Cancellable;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Saves logs published through Kafka.
 */
public final class LogSaver extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(LogSaver.class);
  private static final int TIMEOUT_SECONDS = 10;

  private final String topic;
  private final KafkaClientService kafkaClient;
  private final Set<Integer> partitions;

  private final Map<Integer, Cancellable> kafkaCancelMap;
  private final Map<Integer, CountDownLatch> kafkaCancelCallbackLatchMap;
  private final Set<KafkaLogProcessorFactory> messageProcessorFactories;
  private final SetMultimap<Integer, KafkaLogProcessor> partitionProcessorsMap;

  private final MetricsContext metricsContext;

  @Inject
  LogSaver(KafkaClientService kafkaClient,
           CConfiguration cConf,
           @Named(Constants.LogSaver.MESSAGE_PROCESSOR_FACTORIES)
           Set<KafkaLogProcessorFactory> messageProcessorFactories,
           @Assisted Set<Integer> partitions,
           MetricsCollectionService metricsCollectionService)
    throws Exception {
    LOG.info("Initializing LogSaver...");

    this.topic = cConf.get(Constants.Logging.KAFKA_TOPIC);
    this.partitions = partitions;
    LOG.info(String.format("Kafka topic: %s, partitions: %s", this.topic, this.partitions));

    this.kafkaClient = kafkaClient;
    this.kafkaCancelMap = new HashMap<>();
    this.kafkaCancelCallbackLatchMap = new HashMap<>();
    this.messageProcessorFactories = messageProcessorFactories;
    this.partitionProcessorsMap = HashMultimap.create();

    // TODO: add instance id of the log saver as a tag, when CDAP-3265 is fixed
    this.metricsContext = metricsCollectionService.getContext(
      ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, Id.Namespace.SYSTEM.getId(),
                      Constants.Metrics.Tag.COMPONENT, Constants.Service.LOGSAVER));
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting LogSaver...");
    createProcessors(partitions);
    waitForDatasetAvailability();
    scheduleTasks(partitions);
    LOG.info("Started LogSaver.");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping LogSaver...");
    unscheduleTasks();
    LOG.info("Stopped LogSaver.");
  }

  @VisibleForTesting
  void scheduleTasks(Set<Integer> partitions) throws Exception {
    subscribe(partitions);
 }

  @VisibleForTesting
  void unscheduleTasks() {
    cancelLogCollectorCallbacks();

    for (KafkaLogProcessor processor : partitionProcessorsMap.values()) {
      try {
        // Catching the exception to let all the plugins a chance to stop cleanly.
        processor.stop();
      } catch (Throwable th) {
        LOG.error("Error stopping processor {}",
                  processor.getClass().getSimpleName());
      }
    }
    partitionProcessorsMap.clear();
  }

  private void createProcessors(Set<Integer> partitions) throws Exception {
    for (int partition : partitions) {
      Set<KafkaLogProcessor> processors = new HashSet<>();
      for (KafkaLogProcessorFactory factory : messageProcessorFactories) {
        KafkaLogProcessor logProcessor = factory.create();
        processors.add(logProcessor);
      }
      partitionProcessorsMap.putAll(partition, processors);
    }
  }

  private void cancelLogCollectorCallbacks() {
   for (Entry<Integer, Cancellable> entry : kafkaCancelMap.entrySet()) {
      if (entry.getValue() != null) {
        LOG.info("Cancelling kafka callback for partition {}", entry.getKey());
        kafkaCancelCallbackLatchMap.get(entry.getKey()).countDown();
        entry.getValue().cancel();
      }
    }

    kafkaCancelMap.clear();
    kafkaCancelCallbackLatchMap.clear();
  }

  private void subscribe(Set<Integer> partitions) throws Exception {
    LOG.info("Prepare to subscribe for partitions: {}", partitions);

    Map<Integer, Long> partitionOffset = Maps.newHashMap();
    for (int part : partitions) {
      Set<KafkaLogProcessor> kafkaLogProcessors = partitionProcessorsMap.get(part);
      for (KafkaLogProcessor kafkaLogProcessor : kafkaLogProcessors) {
        kafkaLogProcessor.init(part);
      }

      KafkaConsumer.Preparer preparer = kafkaClient.getConsumer().prepare();
      long offset = getLowestCheckpointOffset(part);
      partitionOffset.put(part, offset);

      if (offset >= 0) {
        preparer.add(topic, part, offset);
      } else {
        preparer.addFromBeginning(topic, part);
      }

      kafkaCancelCallbackLatchMap.put(part, new CountDownLatch(1));

      kafkaCancelMap.put(part, preparer.consume(
        new KafkaMessageCallback(part, kafkaCancelCallbackLatchMap.get(part), kafkaLogProcessors, metricsContext)));
    }

    LOG.info("Consumer created for topic {}, partitions {}", topic, partitionOffset);
  }

  private long getLowestCheckpointOffset(int partition) {
    long lowestCheckpoint = -1L;

    for (KafkaLogProcessor processor : partitionProcessorsMap.get(partition)) {
      Checkpoint checkpoint = processor.getCheckpoint();
      LOG.trace("Got checkpoint {} for partition {} and processor {}",
                checkpoint, partition, processor.getClass().getName());
      // If checkpoint offset is -1; then ignore the checkpoint offset
      if (checkpoint.getNextOffset() != -1) {
        lowestCheckpoint =  (lowestCheckpoint == -1 || checkpoint.getNextOffset() < lowestCheckpoint) ?
                             checkpoint.getNextOffset() :
                             lowestCheckpoint;
      }
    }
    LOG.debug("Lowest checkpoint for partition {} is {}", partition, lowestCheckpoint);
    return lowestCheckpoint;
  }


  private void waitForDatasetAvailability() throws InterruptedException {
    boolean isDatasetAvailable = false;
    while (!isDatasetAvailable) {
      try {
         for (KafkaLogProcessor processor : partitionProcessorsMap.values()) {
           processor.getCheckpoint();
         }
        isDatasetAvailable = true;
      } catch (Exception e) {
        LOG.warn(String.format("Cannot discover dataset service. Retry after %d seconds timeout.", TIMEOUT_SECONDS));
        TimeUnit.SECONDS.sleep(TIMEOUT_SECONDS);
      }
    }
  }
}
