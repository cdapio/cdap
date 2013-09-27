/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.save;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.kafka.client.KafkaClientService;
import com.continuuity.logging.LoggingConfiguration;
import com.continuuity.logging.appender.kafka.KafkaTopic;
import com.continuuity.logging.appender.kafka.LoggingEventSerializer;
import com.continuuity.logging.kafka.KafkaLogEvent;
import com.continuuity.watchdog.election.PartitionChangeHandler;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.common.Threads;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.continuuity.kafka.client.KafkaConsumer.Preparer;

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

  private static final String TABLE_NAME = LoggingConfiguration.LOG_META_DATA_TABLE;
  private final AvroFileWriter avroFileWriter;
  private final ListeningScheduledExecutorService scheduledExecutor;
  private final LogCleanup logCleanup;

  private Cancellable kafkaCancel;
  private ScheduledFuture<?> logWriterFuture;
  private ScheduledFuture<?> cleanupFuture;

  public LogSaver(DataSetAccessor dataSetAccessor, TransactionSystemClient txClient, KafkaClientService kafkaClient,
                  Configuration hConfig, CConfiguration cConfig)
    throws Exception {
    LOG.info("Initializing LogSaver...");

    this.topic = KafkaTopic.getTopic();
    LOG.info(String.format("Kafka topic is %s", this.topic));
    this.serializer = new LoggingEventSerializer();

    OrderedColumnarTable metaTable = getMetaTable(dataSetAccessor);
    this.checkpointManager = new CheckpointManager(metaTable, txClient, topic);
    FileMetaDataManager fileMetaDataManager = new FileMetaDataManager(metaTable, txClient);
    this.messageTable = HashBasedTable.create();

    this.kafkaClient = kafkaClient;

    String baseDir = cConfig.get(LoggingConfiguration.LOG_BASE_DIR);
    Preconditions.checkNotNull(baseDir, "Log base dir cannot be null");
    Path logBaseDir = new Path(baseDir);
    LOG.info(String.format("Log base dir is %s", logBaseDir));

    long retentionDurationDays = cConfig.getLong(LoggingConfiguration.LOG_RETENTION_DURATION_DAYS,
                                                 LoggingConfiguration.DEFAULT_LOG_RETENTION_DURATION_DAYS);
    Preconditions.checkArgument(retentionDurationDays > 0,
                                "Log file retention duration is invalid: %s", retentionDurationDays);
    long retentionDurationMs = TimeUnit.MILLISECONDS.convert(retentionDurationDays, TimeUnit.DAYS);

    long maxLogFileSizeBytes = cConfig.getLong(LoggingConfiguration.LOG_MAX_FILE_SIZE_BYTES, 100 * 1024 * 1024);
    Preconditions.checkArgument(maxLogFileSizeBytes > 0,
                                "Max log file size is invalid: %s", maxLogFileSizeBytes);

    int syncIntervalBytes = cConfig.getInt(LoggingConfiguration.LOG_FILE_SYNC_INTERVAL_BYTES, 5 * 1024 * 1024);
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

    int logCleanupIntervalMins = cConfig.getInt(LoggingConfiguration.LOG_CLEANUP_RUN_INTERVAL_MINS,
                                                LoggingConfiguration.DEFAULT_LOG_CLEANUP_RUN_INTERVAL_MINS);
    Preconditions.checkArgument(logCleanupIntervalMins > 0,
                                "Log cleanup run interval is invalid: %s", logCleanupIntervalMins);

    this.avroFileWriter = new AvroFileWriter(checkpointManager, fileMetaDataManager,
                                             getFileSystem(cConfig, hConfig), logBaseDir,
                                             serializer.getAvroSchema(),
                                             maxLogFileSizeBytes, syncIntervalBytes,
                                             checkpointIntervalMs, inactiveIntervalMs);
    this.scheduledExecutor =
      MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("log-saver-main")));
    this.logCleanup = new LogCleanup(getFileSystem(cConfig, hConfig), fileMetaDataManager,
                                     logBaseDir, retentionDurationMs);

  }

  public static OrderedColumnarTable getMetaTable(DataSetAccessor dataSetAccessor) throws Exception {
    DataSetManager dsManager = dataSetAccessor.getDataSetManager(OrderedColumnarTable.class,
                                                                 DataSetAccessor.Namespace.SYSTEM);
    if (!dsManager.exists(TABLE_NAME)) {
      dsManager.create(TABLE_NAME);
    }

    return dataSetAccessor.getDataSetClient(TABLE_NAME, OrderedColumnarTable.class, DataSetAccessor.Namespace.SYSTEM);
  }

  @Override
  public void partitionsChanged(Set<Integer> partitions) throws Exception {
    unscheduleTasks();
    scheduleTasks(partitions);
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
    logCleanup.close();

    avroFileWriter.checkPoint(true);
    avroFileWriter.close();
  }

  private void scheduleTasks(Set<Integer> partitions) throws Exception {
    // Don't schedule any tasks when not running
    if (!isRunning()) {
      LOG.info("Not scheduling when stopping!");
      return;
    }

    subscribe(partitions);

    LogWriter logWriter = new LogWriter(avroFileWriter, messageTable,
                                        eventProcessingDelayMs, eventBucketIntervalMs);
    logWriterFuture = scheduledExecutor.scheduleWithFixedDelay(logWriter, 100, 200, TimeUnit.MILLISECONDS);

    /*
    if (leaderPartitions.contains(0)) {
      cleanupFuture = scheduledExecutor.scheduleAtFixedRate(logCleanup, 10,
                                                               logCleanupIntervalMins, TimeUnit.MINUTES);
    }
    */
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

    avroFileWriter.checkPoint(true);
    if (kafkaCancel != null) {
      kafkaCancel.cancel();
      kafkaCancel = null;
    }

    messageTable.clear();
  }

  private void subscribe(Set<Integer> partitions) throws Exception {
    LOG.info("Prepare to subscribe.");

    Preparer preparer = kafkaClient.getConsumer().prepare();

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


  private static FileSystem getFileSystem(CConfiguration cConfig, Configuration hConfig) throws Exception {
    String hdfsUser = cConfig.get(Constants.CFG_HDFS_USER);
    FileSystem fileSystem;
    if (hdfsUser == null) {
      LOG.info("Create FileSystem with no user.");
      fileSystem = FileSystem.get(FileSystem.getDefaultUri(hConfig), hConfig);
    } else {
      LOG.info("Create FileSystem with user {}", hdfsUser);
      fileSystem = FileSystem.get(FileSystem.getDefaultUri(hConfig), hConfig, hdfsUser);
    }

    // local file system's hflush() does not work. Using the raw local file system fixes it.
    // https://issues.apache.org/jira/browse/HADOOP-7844
    if (fileSystem instanceof LocalFileSystem) {
      fileSystem = ((LocalFileSystem) fileSystem).getRawFileSystem();
    }

    return fileSystem;
  }
}
