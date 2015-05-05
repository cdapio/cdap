/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.logging.appender.kafka.KafkaTopic;
import co.cask.cdap.watchdog.election.PartitionChangeHandler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.common.Cancellable;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Saves logs published through Kafka.
 */
public final class LogSaver extends AbstractIdleService implements PartitionChangeHandler {
  private static final Logger LOG = LoggerFactory.getLogger(LogSaver.class);
  private static final int TIMEOUT_SECONDS = 10;

  private final String topic;
  private final KafkaClientService kafkaClient;

  private Map<Integer, Cancellable> kafkaCancelMap;
  private Map<Integer, CountDownLatch> kafkaCancelCallbackLatchMap;
  private Set<KafkaLogProcessor> messageProcessors;


  @Inject
  public LogSaver(KafkaClientService kafkaClient,
                  @Named(Constants.LogSaver.MESSAGE_PROCESSORS) Set<KafkaLogProcessor> messageProcessors)
                  throws Exception {
    LOG.info("Initializing LogSaver...");

    this.topic = KafkaTopic.getTopic();
    LOG.info(String.format("Kafka topic is %s", this.topic));

    this.kafkaClient = kafkaClient;
    this.kafkaCancelMap = new HashMap<Integer, Cancellable>();
    this.kafkaCancelCallbackLatchMap = new HashMap<Integer, CountDownLatch>();
    this.messageProcessors = messageProcessors;
  }

  @Override
  public void partitionsChanged(Set<Integer> partitions) {
    try {
      LOG.info("Changed partitions: {}", partitions);
      unscheduleTasks();
      scheduleTasks(partitions);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected void startUp() throws Exception {
    waitForDatasetAvailability();
    LOG.info("Starting LogSaver...");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping LogSaver...");
    // Log saver is stopped by Multi-leader election through unscheduleTasks()
  }

  @VisibleForTesting
  void scheduleTasks(Set<Integer> partitions) throws Exception {
    // Don't schedule any tasks when not running
    if (!isRunning()) {
      LOG.info("Not scheduling when stopping!");
      return;
    }
    subscribe(partitions);
 }

  @VisibleForTesting
  void unscheduleTasks() throws Exception {
    cancelLogCollectorCallbacks();

    for (KafkaLogProcessor processor : messageProcessors) {
      try {
        // Catching the exception to let all the plugins a chance to stop cleanly.
        processor.stop();
      } catch (Throwable th) {
        LOG.error("Error stopping processor {}",
                  processor.getClass().getSimpleName());
      }
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

    for (KafkaLogProcessor processor : messageProcessors) {
      processor.init(partitions);
    }

    Map<Integer, Long> partitionOffset = Maps.newHashMap();
    for (int part : partitions) {
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
        new KafkaMessageCallback(kafkaCancelCallbackLatchMap.get(part), messageProcessors)));
    }

    LOG.info("Consumer created for topic {}, partitions {}", topic, partitionOffset);
  }

  private long getLowestCheckpointOffset(int partition) {
    long lowestCheckpoint = -1L;

    for (KafkaLogProcessor processor : messageProcessors) {
      Checkpoint checkpoint = processor.getCheckpoint(partition);
      // If checkpoint offset is -1; then ignore the checkpoint offset
      if (checkpoint.getNextOffset() != -1) {
        lowestCheckpoint =  (lowestCheckpoint == -1 || checkpoint.getNextOffset() < lowestCheckpoint) ?
                             checkpoint.getNextOffset() :
                             lowestCheckpoint;
      }
    }
    return lowestCheckpoint;
  }


  private void waitForDatasetAvailability() throws InterruptedException {
    boolean isDatasetAvailable = false;
    while (!isDatasetAvailable) {
      try {
         for (KafkaLogProcessor processor : messageProcessors) {
           processor.getCheckpoint(0);
         }
        isDatasetAvailable = true;
      } catch (Exception e) {
        LOG.warn(String.format("Cannot discover dataset service. Retry after %d seconds timeout.", TIMEOUT_SECONDS));
        TimeUnit.SECONDS.sleep(TIMEOUT_SECONDS);
      }
    }
  }
}
