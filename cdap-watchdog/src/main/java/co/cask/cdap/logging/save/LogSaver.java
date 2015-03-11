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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.appender.kafka.KafkaTopic;
import co.cask.cdap.logging.appender.kafka.LoggingEventSerializer;
import co.cask.cdap.logging.kafka.KafkaLogEvent;
import co.cask.cdap.logging.write.AvroFileWriter;
import co.cask.cdap.logging.write.FileMetaDataManager;
import co.cask.cdap.logging.write.LogCleanup;
import co.cask.cdap.logging.write.LogFileWriter;
import co.cask.cdap.watchdog.election.PartitionChangeHandler;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.RowSortedTable;
import com.google.common.collect.TreeBasedTable;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Saves logs published through Kafka.
 */
public final class LogSaver extends AbstractIdleService implements PartitionChangeHandler {
  private static final Logger LOG = LoggerFactory.getLogger(LogSaver.class);
  private static final int TIMEOUT_SECONDS = 10;

  private final String logBaseDir;
  private final String topic;
  private final LoggingEventSerializer serializer;

  private final KafkaClientService kafkaClient;

  private final CheckpointManager checkpointManager;
  private final RowSortedTable<Long, String, Entry<Long, List<KafkaLogEvent>>> messageTable;

  private final long eventBucketIntervalMs;
  private final int logCleanupIntervalMins;
  private final long maxNumberOfBucketsInTable;

  private final LogFileWriter<KafkaLogEvent> logFileWriter;
  private final ListeningScheduledExecutorService scheduledExecutor;
  private final LogCleanup logCleanup;

  private ScheduledFuture<?> logWriterFuture;
  private ScheduledFuture<?> cleanupFuture;

  private Map<Integer, Cancellable> kafkaCancelMap;
  private Map<Integer, CountDownLatch> kafkaCancelCallbackLatchMap;

  @Inject
  public LogSaver(CheckpointManager checkpointManager,
                  FileMetaDataManager fileMetaDataManager, KafkaClientService kafkaClient,
                  CConfiguration cConfig, LocationFactory locationFactory) throws Exception {
    LOG.info("Initializing LogSaver...");

    this.topic = KafkaTopic.getTopic();
    LOG.info(String.format("Kafka topic is %s", this.topic));
    this.serializer = new LoggingEventSerializer();

    this.checkpointManager = checkpointManager;
    this.messageTable = TreeBasedTable.create();

    this.kafkaClient = kafkaClient;

    this.logBaseDir = cConfig.get(LoggingConfiguration.LOG_BASE_DIR);
    Preconditions.checkNotNull(this.logBaseDir, "Log base dir cannot be null");
    LOG.info(String.format("Log base dir is %s", this.logBaseDir));

    long retentionDurationDays = cConfig.getLong(LoggingConfiguration.LOG_RETENTION_DURATION_DAYS,
                                                 LoggingConfiguration.DEFAULT_LOG_RETENTION_DURATION_DAYS);
    Preconditions.checkArgument(retentionDurationDays > 0,
                                "Log file retention duration is invalid: %s", retentionDurationDays);
    long retentionDurationMs = TimeUnit.MILLISECONDS.convert(retentionDurationDays, TimeUnit.DAYS);

    long maxLogFileSizeBytes = cConfig.getLong(LoggingConfiguration.LOG_MAX_FILE_SIZE_BYTES, 20 * 1024 * 1024);
    Preconditions.checkArgument(maxLogFileSizeBytes > 0,
                                "Max log file size is invalid: %s", maxLogFileSizeBytes);

    int syncIntervalBytes = cConfig.getInt(LoggingConfiguration.LOG_FILE_SYNC_INTERVAL_BYTES, 50 * 1024);
    Preconditions.checkArgument(syncIntervalBytes > 0,
                                "Log file sync interval is invalid: %s", syncIntervalBytes);

    long checkpointIntervalMs = cConfig.getLong(LoggingConfiguration.LOG_SAVER_CHECKPOINT_INTERVAL_MS,
                                                LoggingConfiguration.DEFAULT_LOG_SAVER_CHECKPOINT_INTERVAL_MS);
    Preconditions.checkArgument(checkpointIntervalMs > 0,
                                "Checkpoint interval is invalid: %s", checkpointIntervalMs);

    long inactiveIntervalMs = cConfig.getLong(LoggingConfiguration.LOG_SAVER_INACTIVE_FILE_INTERVAL_MS,
                                              LoggingConfiguration.DEFAULT_LOG_SAVER_INACTIVE_FILE_INTERVAL_MS);
    Preconditions.checkArgument(inactiveIntervalMs > 0,
                                "Inactive interval is invalid: %s", inactiveIntervalMs);

    this.eventBucketIntervalMs = cConfig.getLong(LoggingConfiguration.LOG_SAVER_EVENT_BUCKET_INTERVAL_MS,
                                                 LoggingConfiguration.DEFAULT_LOG_SAVER_EVENT_BUCKET_INTERVAL_MS);
    Preconditions.checkArgument(this.eventBucketIntervalMs > 0,
                                "Event bucket interval is invalid: %s", this.eventBucketIntervalMs);

    this.maxNumberOfBucketsInTable = cConfig.getLong
      (LoggingConfiguration.LOG_SAVER_MAXIMUM_INMEMORY_EVENT_BUCKETS,
       LoggingConfiguration.DEFAULT_LOG_SAVER_MAXIMUM_INMEMORY_EVENT_BUCKETS);
    Preconditions.checkArgument(this.maxNumberOfBucketsInTable > 0,
                                "Maximum number of event buckets in memory is invalid: %s",
                                this.maxNumberOfBucketsInTable);


    long topicCreationSleepMs = cConfig.getLong(LoggingConfiguration.LOG_SAVER_TOPIC_WAIT_SLEEP_MS,
                                                LoggingConfiguration.DEFAULT_LOG_SAVER_TOPIC_WAIT_SLEEP_MS);
    Preconditions.checkArgument(topicCreationSleepMs > 0,
                                "Topic creation wait sleep is invalid: %s", topicCreationSleepMs);

    logCleanupIntervalMins = cConfig.getInt(LoggingConfiguration.LOG_CLEANUP_RUN_INTERVAL_MINS,
                                            LoggingConfiguration.DEFAULT_LOG_CLEANUP_RUN_INTERVAL_MINS);
    Preconditions.checkArgument(logCleanupIntervalMins > 0,
                                "Log cleanup run interval is invalid: %s", logCleanupIntervalMins);

    AvroFileWriter avroFileWriter = new AvroFileWriter(fileMetaDataManager, locationFactory.create(""),
                                                       logBaseDir, serializer.getAvroSchema(), maxLogFileSizeBytes,
                                                       syncIntervalBytes, inactiveIntervalMs);

    this.logFileWriter = new CheckpointingLogFileWriter(avroFileWriter, checkpointManager, checkpointIntervalMs);

    this.scheduledExecutor =
      MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("log-saver-main")));
    this.logCleanup = new LogCleanup(fileMetaDataManager, locationFactory.create(""), retentionDurationMs);

    this.kafkaCancelMap = new HashMap<Integer, Cancellable>();
    this.kafkaCancelCallbackLatchMap = new HashMap<Integer, CountDownLatch>();
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

    cancelLogCollectorCallbacks();
    scheduledExecutor.shutdown();

    logFileWriter.flush();
    logFileWriter.close();
  }

  private void scheduleTasks(Set<Integer> partitions) throws Exception {
    // Don't schedule any tasks when not running
    if (!isRunning()) {
      LOG.info("Not scheduling when stopping!");
      return;
    }

    subscribe(partitions);

    LogWriter logWriter = new LogWriter(logFileWriter, messageTable,
                                        eventBucketIntervalMs, maxNumberOfBucketsInTable);
    logWriterFuture = scheduledExecutor.scheduleWithFixedDelay(logWriter, 100, 200, TimeUnit.MILLISECONDS);

    if (partitions.contains(0)) {
      LOG.info("Scheduling cleanup task");
      cleanupFuture = scheduledExecutor.scheduleAtFixedRate(logCleanup, 10,
                                                            logCleanupIntervalMins, TimeUnit.MINUTES);
    }
  }

  private void unscheduleTasks() throws Exception {
    if (logWriterFuture != null && !logWriterFuture.isCancelled() && !logWriterFuture.isDone()) {
      logWriterFuture.cancel(false);
      logWriterFuture = null;
    }

    if (cleanupFuture != null && !cleanupFuture.isCancelled() && !cleanupFuture.isDone()) {
      cleanupFuture.cancel(false);
      cleanupFuture = null;
    }

    logFileWriter.flush();

    cancelLogCollectorCallbacks();

    messageTable.clear();
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
      KafkaConsumer.Preparer preparer = kafkaClient.getConsumer().prepare();
      long offset = checkpointManager.getCheckpoint(part);
      partitionOffset.put(part, offset);

      if (offset >= 0) {
        preparer.add(topic, part, offset);
      } else {
        preparer.addFromBeginning(topic, part);
      }

      kafkaCancelCallbackLatchMap.put(part, new CountDownLatch(1));
      kafkaCancelMap.put(part, preparer.consume(
        new LogCollectorCallback(messageTable, serializer,
                                 eventBucketIntervalMs, maxNumberOfBucketsInTable,
                                 kafkaCancelCallbackLatchMap.get(part), logBaseDir)));
    }

    LOG.info("Consumer created for topic {}, partitions {}", topic, partitionOffset);
  }

  private void waitForDatasetAvailability() throws InterruptedException {
    boolean isDatasetAvailable = false;
    while (!isDatasetAvailable) {
      try {
        checkpointManager.getCheckpoint(0);
        isDatasetAvailable = true;
      } catch (Exception e) {
        LOG.warn(String.format("Cannot discover dataset service. Retry after %d seconds timeout.", TIMEOUT_SECONDS));
        TimeUnit.SECONDS.sleep(TIMEOUT_SECONDS);
      }
    }
  }
}
