/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.save;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.LoggingConfiguration;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.logging.logback.kafka.KafkaTopic;
import com.continuuity.common.logging.logback.kafka.LoggingEventSerializer;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.logging.LoggingContextLookup;
import com.continuuity.logging.kafka.Callback;
import com.continuuity.logging.kafka.KafkaConsumer;
import com.continuuity.logging.kafka.KafkaLogEvent;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.continuuity.common.logging.LoggingConfiguration.KafkaHost;
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

  private final OperationExecutor opex;
  private final OperationContext operationContext;
  private final CheckpointManager checkpointManager;
  private final FileManager fileManager;
  private final Table<Long, String, List<KafkaLogEvent>> messageTable;
  private final AvroFileWriter avroFileWriter;
  private final long kafkaErrorSleepMs = 2000;
  private final long kafkaEmptySleepMs = 2000;
  private final int kafkaSaveFetchTimeoutMs = 1000;
  private final int syncIntervalBytes = 1024 * 1024;
  private final long maxLogFileSize = 100 * 1024 * 1024;
  private final long checkpointIntervalMs = 60 * 1000;
  private final long inactiveIntervalMs = 10 * 60 * 1000;
  private final long eventProcessingDelayMs = 5 * 1000;
  private final int fetchSizeBytes = 1024 * 1024;

  private final int numThreads = 2;
  private static final String TABLE_NAME = "__log_meta";

  private volatile ListeningExecutorService listeningExecutorService;
  private volatile Future<?> subTaskFutures;

  public LogSaver(OperationExecutor opex, int partition, Configuration hConfig, CConfiguration cConfig)
    throws IOException {
    LOG.info("Initializing LogSaver...");

    Preconditions.checkNotNull(opex, "Opex cannot be null");

    String kafkaSeedBrokers = cConfig.get(LoggingConfiguration.KAFKA_SEED_BROKERS);
    Preconditions.checkArgument(kafkaSeedBrokers != null && !kafkaSeedBrokers.isEmpty(),
      "Kafka seed brokers config is not available");
    this.seedBrokers = LoggingConfiguration.getKafkaSeedBrokers(kafkaSeedBrokers);
    Preconditions.checkNotNull(this.seedBrokers, "Not able to parse Kafka seed brokers");

    String account = cConfig.get(LoggingConfiguration.LOG_SAVER_RUN_ACCOUNT);
    Preconditions.checkNotNull(account, "Account cannot be null");

    this.topic = KafkaTopic.getTopic();
    this.partition = partition;
    this.serializer = new LoggingEventSerializer();

    this.opex = opex;
    this.operationContext = new OperationContext(account);
    this.checkpointManager = new CheckpointManager(this.opex, operationContext, topic, partition, TABLE_NAME);
    this.fileManager = new FileManager(opex, operationContext, TABLE_NAME);
    this.messageTable = HashBasedTable.create();

    String logBaseDir = cConfig.get(LoggingConfiguration.LOG_BASE_DIR);
    Preconditions.checkNotNull(logBaseDir, "Log base dir cannot be null");
    this.avroFileWriter = new AvroFileWriter(checkpointManager, fileManager,
                                             FileSystem.get(hConfig), logBaseDir,
                                             serializer.getAvroSchema(), maxLogFileSize, syncIntervalBytes,
                                             checkpointIntervalMs, inactiveIntervalMs);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting LogSaver...");

    listeningExecutorService =
      MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numThreads));
    List<ListenableFuture<?>> futures = Lists.newArrayList();

    ListenableFuture<?> future = listeningExecutorService.submit(new LogCollector());
    futures.add(future);

    future = listeningExecutorService.submit(new LogWriter());
    futures.add(future);

    subTaskFutures = Futures.allAsList(futures);
  }

  @Override
  protected void shutDown() throws Exception {
    // Wait for sub tasks to complete
    subTaskFutures.get();

    LOG.info("Stopping LogSaver...");

    listeningExecutorService.shutdownNow();
  }

  private void waitForRun() {
    while (state() == State.STARTING) {
      try {
        TimeUnit.MILLISECONDS.sleep(200);
      } catch (InterruptedException e) {
        LOG.warn("Caught exception while waiting for service to start", e);
      }
    }
  }

  private final class LogCollector implements Runnable, Callback {
    long lastOffset;

    @Override
    public void run() {
      waitForRun();

      KafkaConsumer kafkaConsumer = new KafkaConsumer(seedBrokers, topic, partition, kafkaSaveFetchTimeoutMs);
      try {
        CheckpointInfo checkpointInfo = checkpointManager.getCheckpoint();
        lastOffset = checkpointInfo == null ? -1 : checkpointInfo.getOffset();
        LOG.info(String.format("Starting LogCollector for topic %s, partition %d.", topic, partition));

        while (isRunning()) {
          try {
            int msgCount = kafkaConsumer.fetchMessages(lastOffset + 1, fetchSizeBytes, this);
            if (msgCount == 0) {
              LOG.info(String.format("No more messages in topic %s, partition %d. Will sleep for %d ms",
                                     topic, partition, kafkaEmptySleepMs));
              TimeUnit.MILLISECONDS.sleep(kafkaEmptySleepMs);
            }

            LOG.info(String.format("Got %d log messages from Kafka for topic %s, partition %s",
                                   msgCount, topic, partition));
          } catch (Throwable e) {
            LOG.error(
              String.format("Caught exception during fetch of topic %s, partition %d, will try again after %d ms:",
                            topic, partition, kafkaErrorSleepMs), e);
            try {
              TimeUnit.MILLISECONDS.sleep(kafkaErrorSleepMs);
            } catch (InterruptedException e1) {
              LOG.error(String.format("Caught InterruptedException for topic %s, partition %d",
                                      topic, partition), e1);
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
      GenericRecord genericRecord = serializer.toGenericRecord(msgBuffer);
      ILoggingEvent event = serializer.fromGenericRecord(genericRecord);
      LoggingContext loggingContext = LoggingContextLookup.getLoggingContext(event.getMDCPropertyMap());
      if (loggingContext == null) {
        LOG.debug(String.format("Logging context is not set for event %s. Skipping it.", event));
        return;
      }

      synchronized (messageTable) {
        long key = event.getTimeStamp() / eventProcessingDelayMs;
        List<KafkaLogEvent> msgList = messageTable.get(key, loggingContext.getLogPathFragment());
        if (msgList == null) {
          msgList = Lists.newArrayList();
          messageTable.put(key, loggingContext.getLogPathFragment(), msgList);
        }
        msgList.add(new KafkaLogEvent(genericRecord, event, loggingContext, offset));
      }
      lastOffset = offset;
    }
  }

  private final class LogWriter implements Runnable {
    @Override
    public void run() {
      waitForRun();

      LOG.info(String.format("Starting LogWriter for topic %s, partition %d.", topic, partition));

      List<List<KafkaLogEvent>> writeLists = Lists.newArrayList();
      try {
        while (isRunning()) {
          int messages = 0;
          writeLists.clear();
          try {
            long processKey = (System.currentTimeMillis() - eventProcessingDelayMs) / eventProcessingDelayMs;
            synchronized (messageTable) {
              for (Iterator<Table.Cell<Long, String, List<KafkaLogEvent>>> it = messageTable.cellSet().iterator();
                   it.hasNext(); ) {
                Table.Cell<Long, String, List<KafkaLogEvent>> cell = it.next();
                // Process only messages older than eventProcessingDelayMs
                if (cell.getRowKey() >= processKey) {
                  continue;
                }
                writeLists.add(cell.getValue());
                it.remove();
                messages += cell.getValue().size();
              }
            }
            if (writeLists.isEmpty()) {
              LOG.info(String.format("No more messages to save for topic %s, partition %d. Will sleep for %d ms",
                                     topic, partition, kafkaEmptySleepMs));
              TimeUnit.MILLISECONDS.sleep(kafkaEmptySleepMs);
            }

            LOG.info(String.format("Got %d log messages to save for topic %s, partition %s",
                                   messages, topic, partition));
            for (List<KafkaLogEvent> list : writeLists) {
              avroFileWriter.append(list);
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
            }
          }
        }

        LOG.info(String.format("Stopping LogWriter for topic %s, partition %d.", topic, partition));
      } finally {
        try {
          avroFileWriter.close();
        } catch (IOException e) {
          LOG.error(String.format("Caught exception while closing AvroFileWriter for topic %s, partition %d:",
                                  topic, partition), e);
        }
      }
    }
  }
}
