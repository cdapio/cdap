/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.save;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.logging.LoggingConfiguration;
import com.continuuity.logging.appender.kafka.KafkaTopic;
import com.continuuity.logging.appender.kafka.LoggingEventSerializer;
import com.continuuity.logging.context.LoggingContextHelper;
import com.continuuity.logging.kafka.Callback;
import com.continuuity.logging.kafka.KafkaConsumer;
import com.continuuity.logging.kafka.KafkaLogEvent;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import kafka.common.OffsetOutOfRangeException;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.continuuity.logging.LoggingConfiguration.KafkaHost;
import static com.continuuity.logging.save.CheckpointManager.CheckpointInfo;

/**
 * Saves logs published through Kafka.
 */
@SuppressWarnings("FieldCanBeLocal")
public final class LogSaver extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(LogSaver.class);

  private final List<KafkaHost> seedBrokers;
  private final String topic;
  private final int partition;
  private final LoggingEventSerializer serializer;
  private final Path logBaseDir;

  private final CConfiguration cConfig;
  private final Configuration hConfig;
  private final OperationExecutor opex;
  private final OperationContext operationContext;
  private final CheckpointManager checkpointManager;
  private final FileMetaDataManager fileMetaDataManager;
  private final Table<Long, String, List<KafkaLogEvent>> messageTable;
  private final long kafkaErrorSleepMs = 2000;
  private final long kafkaEmptySleepMs = 2000;
  private final int kafkaSaveFetchTimeoutMs = 1000;
  private final int syncIntervalBytes;
  private final long checkpointIntervalMs;
  private final long inactiveIntervalMs;
  private final long eventBucketIntervalMs;
  private final long eventProcessingDelayMs;
  private final long retentionDurationMs;
  private final long maxLogFileSizeBytes;

  private final int numThreads = 2;
  private static final String TABLE_NAME = LoggingConfiguration.LOG_META_DATA_TABLE;

  private volatile ListeningScheduledExecutorService listeningScheduledExecutorService;
  private volatile Future<?> subTaskFutures;
  private volatile ScheduledFuture<?> scheduledFutures;

  public LogSaver(OperationExecutor opex, int partition, Configuration hConfig, CConfiguration cConfig)
    throws IOException {
    LOG.info("Initializing LogSaver...");

    Preconditions.checkNotNull(opex, "Opex cannot be null");

    String kafkaSeedBrokers = cConfig.get(LoggingConfiguration.KAFKA_SEED_BROKERS);
    Preconditions.checkArgument(kafkaSeedBrokers != null && !kafkaSeedBrokers.isEmpty(),
      "Kafka seed brokers config is not available");
    this.seedBrokers = LoggingConfiguration.getKafkaSeedBrokers(kafkaSeedBrokers);
    Preconditions.checkNotNull(this.seedBrokers, "Not able to parse Kafka seed brokers");
    LOG.info(String.format("Kafka seed brokers are %s", kafkaSeedBrokers));

    String account = cConfig.get(LoggingConfiguration.LOG_RUN_ACCOUNT);
    Preconditions.checkNotNull(account, "Account cannot be null");

    this.topic = KafkaTopic.getTopic();
    LOG.info(String.format("Kafka topic is %s", this.topic));
    this.partition = partition;
    LOG.info(String.format("Kafka partition is %d", partition));
    this.serializer = new LoggingEventSerializer();

    this.cConfig = cConfig;
    this.hConfig = hConfig;
    this.opex = opex;
    this.operationContext = new OperationContext(account);
    this.checkpointManager = new CheckpointManager(this.opex, operationContext, topic, partition, TABLE_NAME);
    this.fileMetaDataManager = new FileMetaDataManager(opex, operationContext, TABLE_NAME);
    this.messageTable = HashBasedTable.create();

    String baseDir = cConfig.get(LoggingConfiguration.LOG_BASE_DIR);
    Preconditions.checkNotNull(baseDir, "Log base dir cannot be null");
    this.logBaseDir = new Path(baseDir);
    LOG.info(String.format("Log base dir is %s", logBaseDir));

    long retentionDurationDays = cConfig.getLong(LoggingConfiguration.LOG_RETENTION_DURATION_DAYS,
                                                 LoggingConfiguration.DEFAULT_LOG_RETENTION_DURATION_DAYS);
    Preconditions.checkArgument(retentionDurationDays > 0,
                                "Log file retention duration is invalid: %s", retentionDurationDays);
    this.retentionDurationMs = TimeUnit.MILLISECONDS.convert(retentionDurationDays, TimeUnit.DAYS);

    this.maxLogFileSizeBytes = cConfig.getLong(LoggingConfiguration.LOG_MAX_FILE_SIZE_BYTES, 100 * 1024 * 1024);
    Preconditions.checkArgument(maxLogFileSizeBytes > 0,
                                "Max log file size is invalid: %s", maxLogFileSizeBytes);

    this.syncIntervalBytes = cConfig.getInt(LoggingConfiguration.LOG_FILE_SYNC_INTERVAL_BYTES, 5 * 1024 * 1024);
    Preconditions.checkArgument(this.syncIntervalBytes > 0,
                                "Log file sync interval is invalid: %s", this.syncIntervalBytes);

    this.checkpointIntervalMs = cConfig.getLong(LoggingConfiguration.LOG_SAVER_CHECKPOINT_INTERVAL_MS,
                                                LoggingConfiguration.DEFAULT_LOG_SAVER_CHECKPOINT_INTERVAL_MS);
    Preconditions.checkArgument(this.checkpointIntervalMs > 0,
                                "Checkpoint interval is invalid: %s", this.checkpointIntervalMs);

    this.inactiveIntervalMs = cConfig.getLong(LoggingConfiguration.LOG_SAVER_INACTIVE_FILE_INTERVAL_MS,
                                              LoggingConfiguration.DEFAULT_LOG_SAVER_INACTIVE_FILE_INTERVAL_MS);
    Preconditions.checkArgument(this.inactiveIntervalMs > 0,
                                "Inactive interval is invalid: %s", this.inactiveIntervalMs);

    this.eventBucketIntervalMs = cConfig.getLong(LoggingConfiguration.LOG_SAVER_EVENT_BUCKET_INTERVAL_MS,
                                                 LoggingConfiguration.DEFAULT_LOG_SAVER_EVENT_BUCKET_INTERVAL_MS);
    Preconditions.checkArgument(this.eventBucketIntervalMs > 0,
                                "Event bucket interval is invalid: %s", this.eventBucketIntervalMs);

    this.eventProcessingDelayMs = cConfig.getLong(LoggingConfiguration.LOG_SAVER_EVENT_PROCESSING_DELAY_MS,
                                                  LoggingConfiguration.DEFAULT_LOG_SAVER_EVENT_PROCESSING_DELAY_MS);
    Preconditions.checkArgument(this.eventProcessingDelayMs > 0,
                                "Event processing delay interval is invalid: %s", this.eventProcessingDelayMs);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting LogSaver...");

    listeningScheduledExecutorService =
      MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(numThreads));
    List<ListenableFuture<?>> futures = Lists.newArrayList();

    ListenableFuture<?> future = listeningScheduledExecutorService.submit(new LogCollector());
    futures.add(future);

    future = listeningScheduledExecutorService.submit(new LogWriter(hConfig));
    futures.add(future);

    subTaskFutures = Futures.allAsList(futures);

    scheduledFutures =
      listeningScheduledExecutorService.scheduleAtFixedRate(
        new LogCleanup(getFileSystem(cConfig, hConfig), fileMetaDataManager, logBaseDir, retentionDurationMs),
        10, 24 * 60, TimeUnit.MINUTES);
  }

  @Override
  protected void shutDown() throws Exception {
    // Wait for sub tasks to complete
    subTaskFutures.get();
    scheduledFutures.cancel(false);

    LOG.info("Stopping LogSaver...");

    listeningScheduledExecutorService.shutdownNow();
  }

  private final class LogCollector implements Runnable, Callback {
    long lastOffset;

    @Override
    public void run() {
      KafkaConsumer kafkaConsumer = new KafkaConsumer(seedBrokers, topic, partition, kafkaSaveFetchTimeoutMs);
      try {
        // Wait for service to start
        start().get();

        CheckpointInfo checkpointInfo = checkpointManager.getCheckpoint();
        lastOffset = checkpointInfo == null ? -1 : checkpointInfo.getOffset();
        LOG.info(String.format("Starting LogCollector for topic %s, partition %d, offset %d.",
                               topic, partition, lastOffset));

        while (isRunning()) {
          try {
            int msgCount = kafkaConsumer.fetchMessages(lastOffset + 1, this);
            if (msgCount == 0) {
              LOG.debug("Got 0 messages from Kafka, sleeping...");
              TimeUnit.MILLISECONDS.sleep(kafkaEmptySleepMs);
            } else {
              LOG.debug(String.format("Processed %d log messages from Kafka for topic %s, partition %s, offset %d",
                                      msgCount, topic, partition, lastOffset));
            }
          } catch (OffsetOutOfRangeException e) {

            // Reset offset to earliest available
            long earliestOffset = kafkaConsumer.fetchOffset(KafkaConsumer.Offset.EARLIEST);
            LOG.warn(String.format("Offset %d is out of range. Resetting to earliest available offset %d",
                                   lastOffset + 1, earliestOffset));
            lastOffset = earliestOffset - 1;

          } catch (Throwable e) {
            LOG.error(
              String.format("Caught exception during fetch of topic %s, partition %d, will try again after %d ms:",
                            topic, partition, kafkaErrorSleepMs), e);
            try {
              TimeUnit.MILLISECONDS.sleep(kafkaErrorSleepMs);
            } catch (InterruptedException e1) {
              LOG.error(String.format("Caught InterruptedException for topic %s, partition %d",
                                      topic, partition), e1);
              Thread.currentThread().interrupt();
            }
          }
        }

        LOG.info(String.format("Stopping LogCollector for topic %s, partition %d.", topic, partition));
      } catch (Throwable e) {
        LOG.error("Caught unexpected exception. Terminating...", e);
      } finally {
        try {
          kafkaConsumer.close();
        } catch (IOException e) {
          LOG.error(String.format("Caught exception while closing KafkaConsumer for topic %s, partition %d:",
                                  topic, partition), e);
        }
      }
    }

    @Override
    public void handle(long offset, ByteBuffer msgBuffer) {
      try {
        GenericRecord genericRecord = serializer.toGenericRecord(msgBuffer);
        ILoggingEvent event = serializer.fromGenericRecord(genericRecord);
        LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(event.getMDCPropertyMap());

        synchronized (messageTable) {
          long key = event.getTimeStamp() / eventBucketIntervalMs;
          List<KafkaLogEvent> msgList = messageTable.get(key, loggingContext.getLogPathFragment());
          if (msgList == null) {
            msgList = Lists.newArrayList();
            messageTable.put(key, loggingContext.getLogPathFragment(), msgList);
          }
          msgList.add(new KafkaLogEvent(genericRecord, event, loggingContext, offset));
        }
        lastOffset = offset;
      } catch (Exception e) {
        LOG.warn(String.format("Exception while processing message with offset %d. Skipping it.", offset));
      }
    }
  }

  private final class LogWriter implements Runnable {
    private final FileSystem fileSystem;

    private LogWriter(Configuration hConfig) throws Exception {
      this.fileSystem = getFileSystem(cConfig, hConfig);
    }

    @Override
    public void run() {
      LOG.info(String.format("Starting LogWriter for topic %s, partition %d.", topic, partition));

      AvroFileWriter avroFileWriter = new AvroFileWriter(checkpointManager, fileMetaDataManager,
                                                         fileSystem, logBaseDir,
                                                         serializer.getAvroSchema(),
                                                         maxLogFileSizeBytes, syncIntervalBytes,
                                                         checkpointIntervalMs, inactiveIntervalMs);
      ListMultimap<String, KafkaLogEvent> writeListMap = ArrayListMultimap.create();
      int messages = 0;
      try {
        // Wait for service to start
        start().get();

        while (isRunning()) {
          try {
            // Read new messages only if previous write was successful.
            if (writeListMap.isEmpty()) {
              messages = 0;
              long processKey = (System.currentTimeMillis() - eventProcessingDelayMs) / eventBucketIntervalMs;
              synchronized (messageTable) {
                for (Iterator<Table.Cell<Long, String, List<KafkaLogEvent>>> it = messageTable.cellSet().iterator();
                     it.hasNext(); ) {
                  Table.Cell<Long, String, List<KafkaLogEvent>> cell = it.next();
                  // Process only messages older than eventProcessingDelayMs
                  if (cell.getRowKey() >= processKey) {
                    continue;
                  }

                  writeListMap.putAll(cell.getColumnKey(), cell.getValue());
                  messages += cell.getValue().size();
                  it.remove();
                }
              }
            }
            if (writeListMap.isEmpty()) {
              LOG.debug("Got 0 messages to save, sleeping...");
              TimeUnit.MILLISECONDS.sleep(kafkaEmptySleepMs);
            } else {
              LOG.debug(String.format("Got %d log messages to save for topic %s, partition %s",
                                      messages, topic, partition));
            }

            for (Iterator<String> it = writeListMap.keySet().iterator(); it.hasNext(); ) {
              String key = it.next();
              List<KafkaLogEvent> list = writeListMap.get(key);
              Collections.sort(list);
              avroFileWriter.append(list);
              // Remove successfully written message
              it.remove();
            }
          } catch (Throwable e) {
            LOG.error(
              String.format("Caught exception during save of topic %s, partition %d, will try again after %d ms:",
                            topic, partition, kafkaErrorSleepMs), e);
            try {
              TimeUnit.MILLISECONDS.sleep(kafkaErrorSleepMs);
            } catch (InterruptedException e1) {
              LOG.error(String.format("Caught InterruptedException for topic %s, partition %d",
                                      topic, partition), e1);
              Thread.currentThread().interrupt();
            }
          }
        }

        LOG.info(String.format("Stopping LogWriter for topic %s, partition %d.", topic, partition));
      } catch (InterruptedException e) {
        LOG.error(String.format("Caught InterruptedException for topic %s, partition %d",
                                topic, partition), e);
        Thread.currentThread().interrupt();
      } catch (Throwable t) {
        LOG.error("Caught unexpected exception. Terminating...", t);
      } finally {
        try {
          try {
            avroFileWriter.close();
          } finally {
            fileSystem.close();
          }
        } catch (IOException e) {
          LOG.error(String.format("Caught exception while closing objects for topic %s, partition %d:",
                                  topic, partition), e);
        }
      }
    }
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
