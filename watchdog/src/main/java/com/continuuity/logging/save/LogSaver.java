/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.save;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.logging.LoggingConfiguration;
import com.continuuity.logging.appender.kafka.KafkaTopic;
import com.continuuity.logging.appender.kafka.LoggingEventSerializer;
import com.continuuity.logging.kafka.KafkaLogEvent;
import com.continuuity.logging.write.AvroFileWriter;
import com.continuuity.logging.write.FileMetaDataManager;
import com.continuuity.logging.write.LogCleanup;
import com.continuuity.logging.write.LogFileWriter;
import com.continuuity.watchdog.election.PartitionChangeHandler;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Saves logs published through Kafka.
 */
public final class LogSaver extends AbstractIdleService implements PartitionChangeHandler {
  private static final Logger LOG = LoggerFactory.getLogger(LogSaver.class);

  private final String topic;
  private final LoggingEventSerializer serializer;

  private final KafkaClientService kafkaClient;

  private final CheckpointManager checkpointManager;
  private final Table<Long, String, List<KafkaLogEvent>> messageTable;

  private final long eventBucketIntervalMs;
  private final long eventProcessingDelayMs;
  private final int logCleanupIntervalMins;

  private final LogFileWriter<KafkaLogEvent> logFileWriter;
  private final ListeningScheduledExecutorService scheduledExecutor;
  private final LogCleanup logCleanup;

  private Cancellable kafkaCancel;
  private ScheduledFuture<?> logWriterFuture;
  private ScheduledFuture<?> cleanupFuture;

  @Inject

  public LogSaver(LogSaverTableUtil tableUtil, TransactionSystemClient txClient, KafkaClientService kafkaClient,
                  CConfiguration cConfig, LocationFactory locationFactory) throws Exception {
    LOG.info("Initializing LogSaver...");

    this.topic = KafkaTopic.getTopic();
    LOG.info(String.format("Kafka topic is %s", this.topic));
    this.serializer = new LoggingEventSerializer();

    OrderedColumnarTable metaTable = tableUtil.getMetaTable();
    this.checkpointManager = new CheckpointManager(metaTable, txClient, topic);
    FileMetaDataManager fileMetaDataManager = new FileMetaDataManager(metaTable, txClient, locationFactory);
    this.messageTable = HashBasedTable.create();

    this.kafkaClient = kafkaClient;

    String baseDir = cConfig.get(LoggingConfiguration.LOG_BASE_DIR);
    Preconditions.checkNotNull(baseDir, "Log base dir cannot be null");
    Location logBaseDir = locationFactory.create(baseDir);
    LOG.info(String.format("Log base dir is %s", logBaseDir.toURI()));

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

    this.eventProcessingDelayMs = cConfig.getLong(LoggingConfiguration.LOG_SAVER_EVENT_PROCESSING_DELAY_MS,
                                                  LoggingConfiguration.DEFAULT_LOG_SAVER_EVENT_PROCESSING_DELAY_MS);
    Preconditions.checkArgument(this.eventProcessingDelayMs > 0,
                                "Event processing delay interval is invalid: %s", this.eventProcessingDelayMs);

    long topicCreationSleepMs = cConfig.getLong(LoggingConfiguration.LOG_SAVER_TOPIC_WAIT_SLEEP_MS,
                                                LoggingConfiguration.DEFAULT_LOG_SAVER_TOPIC_WAIT_SLEEP_MS);
    Preconditions.checkArgument(topicCreationSleepMs > 0,
                                "Topic creation wait sleep is invalid: %s", topicCreationSleepMs);

    logCleanupIntervalMins = cConfig.getInt(LoggingConfiguration.LOG_CLEANUP_RUN_INTERVAL_MINS,
                                            LoggingConfiguration.DEFAULT_LOG_CLEANUP_RUN_INTERVAL_MINS);
    Preconditions.checkArgument(logCleanupIntervalMins > 0,
                                "Log cleanup run interval is invalid: %s", logCleanupIntervalMins);

    AvroFileWriter avroFileWriter = new AvroFileWriter(fileMetaDataManager,
                                                       logBaseDir,
                                                       serializer.getAvroSchema(),
                                                       maxLogFileSizeBytes, syncIntervalBytes,
                                                       inactiveIntervalMs);

    this.logFileWriter = new CheckpointingLogFileWriter(avroFileWriter, checkpointManager, checkpointIntervalMs);

    this.scheduledExecutor =
      MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("log-saver-main")));
    this.logCleanup = new LogCleanup(fileMetaDataManager, logBaseDir, retentionDurationMs);

  }

  @Override
  public void partitionsChanged(Set<Integer> partitions) {
    try {
      unscheduleTasks();
      scheduleTasks(partitions);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting LogSaver...");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping LogSaver...");

    kafkaCancel.cancel();
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
                                        eventProcessingDelayMs, eventBucketIntervalMs);
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
    if (kafkaCancel != null) {
      kafkaCancel.cancel();
      kafkaCancel = null;
    }

    messageTable.clear();
  }

  private void subscribe(Set<Integer> partitions) throws Exception {
    LOG.info("Prepare to subscribe.");

    KafkaConsumer.Preparer preparer = kafkaClient.getConsumer().prepare();

    Map<Integer, Long> partitionOffset = Maps.newHashMap();
    for (int part : partitions) {
      long offset = checkpointManager.getCheckpoint(part);
      partitionOffset.put(part, offset);

      if (offset >= 0) {
        preparer.add(topic, part, offset);
      } else {
        preparer.addFromBeginning(topic, part);
      }
    }

    kafkaCancel = preparer.consume(
      new LogCollectorCallback(messageTable, serializer, eventBucketIntervalMs));

    LOG.info("Consumer created for topic {}, partitions {}", topic, partitionOffset);
  }
}
