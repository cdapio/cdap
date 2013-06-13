package com.continuuity.logging.save;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.logging.logback.kafka.LoggingEventSerializer;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.logging.LoggingContextLookup;
import com.continuuity.logging.kafka.KafkaConsumer;
import com.continuuity.logging.kafka.KafkaLogEvent;
import com.continuuity.logging.kafka.KafkaMessage;
import com.google.common.collect.Lists;
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
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Saves logs published through Kafka.
 */
public final class LogSaver extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(LogSaver.class);

  private final List<ImmutablePair<String, Integer>> seedBrokers;
  private final String topic;
  private final int partition;
  private final LoggingEventSerializer serializer;

  private final OperationExecutor opex;
  private final OperationContext operationContext;
  private final CheckpointManager checkpointManager;
  private final FileManager fileManager;
  private final QueueManager queueManager;
  private final AvroFileWriter avroFileWriter;
  private final long kafkaErrorSleepMs;
  private final long kafkaEmptySleepMs;
  private final int kafkaSaveFetchTimeoutMs = 1000;
  private final int syncIntervalBytes = 1024 * 1024;
  private final long maxLogFileSize = 100 * 1024 * 1024;
  private final long checkpointIntervalMs = 60 * 1000;
  private final long inactiveIntervalMs = 10 * 60 * 1000;

  private final int numThreads = 2;
  private static final String TABLE_NAME = "__log_meta";

  private volatile ListeningExecutorService listeningExecutorService;
  private volatile Future<?> subTaskFutures;

  public LogSaver(OperationExecutor opex, String account) throws IOException, URISyntaxException {
    this.seedBrokers = Lists.newArrayList(new ImmutablePair<String, Integer>("localhost", 9094));
    this.topic = "LOG_MESSAGES";
    this.partition = 0;
    this.serializer = new LoggingEventSerializer();

    this.opex = opex;
    this.operationContext = new OperationContext(account);
    this.checkpointManager = new CheckpointManager(this.opex, operationContext, topic, partition, TABLE_NAME);
    this.fileManager = new FileManager(opex, operationContext, TABLE_NAME);
    this.queueManager = new QueueManager(3000);
    this.avroFileWriter = new AvroFileWriter(checkpointManager, fileManager,
                                             FileSystem.get(new Configuration()), "/tmp/logs",
                                             serializer.getSchema(), maxLogFileSize, syncIntervalBytes,
                                             checkpointIntervalMs, inactiveIntervalMs);
    this.kafkaErrorSleepMs = 2000;
    this.kafkaEmptySleepMs = 2000;
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

  private final class LogCollector implements Runnable {
    @Override
    public void run() {
      waitForRun();

      KafkaConsumer kafkaConsumer = new KafkaConsumer(seedBrokers, topic, partition, kafkaSaveFetchTimeoutMs);
      try {
        long lastOffset = -1;

        List<KafkaMessage> messageList = Lists.newArrayListWithExpectedSize(1000);
        LOG.info(String.format("Starting LogCollector for topic %s, partition %d.", topic, partition));

        while (isRunning()) {
          try {
            messageList.clear();
            kafkaConsumer.fetchMessages(lastOffset + 1, messageList);
            if (messageList.isEmpty()) {
              LOG.info(String.format("No more messages in topic %s, partition %d. Will sleep for %d ms",
                                     topic, partition, kafkaEmptySleepMs));
              TimeUnit.MILLISECONDS.sleep(kafkaEmptySleepMs);
            }

            LOG.info(String.format("Got %d log messages from Kafka for topic %s, partition %s",
                                   messageList.size(), topic, partition));
            for (KafkaMessage message : messageList) {
              GenericRecord genericRecord = serializer.toGenericRecord(message.getByteBuffer().array());
              ILoggingEvent event = serializer.fromGenericRecord(genericRecord);
              LoggingContext loggingContext = LoggingContextLookup.getLoggingContext(event.getMDCPropertyMap());
              if (loggingContext == null) {
                LOG.debug(String.format("Logging context is not set for event %s. Skipping it.", event));
                continue;
              }
              queueManager.addLogEvent(new KafkaLogEvent(genericRecord, event, message.getOffset(), loggingContext));
            }

            if (!messageList.isEmpty()) {
              lastOffset = messageList.get(messageList.size() - 1).getOffset();
            }
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
      } finally {
        try {
          kafkaConsumer.close();
        } catch (IOException e) {
          LOG.error(String.format("Caught exception while closing KafkaConsumer for topic %s, partition %d:",
                                  topic, partition), e);
        }
      }
    }
  }

  private final class LogWriter implements Runnable {
    @Override
    public void run() {
      waitForRun();

      int batchSize = 1000;
      List<KafkaLogEvent> events = Lists.newArrayListWithExpectedSize(batchSize);
      LOG.info(String.format("Starting LogWriter for topic %s, partition %d.", topic, partition));

      try {
        while (isRunning()) {
          try {
            events.clear();
            queueManager.drainLogEvents(events, batchSize);
            if (events.isEmpty()) {
              LOG.info(String.format("No more messages to save for topic %s, partition %d. Will sleep for %d ms",
                                     topic, partition, kafkaEmptySleepMs));
              TimeUnit.MILLISECONDS.sleep(kafkaEmptySleepMs);
            }

            LOG.info(String.format("Got %d log messages to save for topic %s, partition %s",
                                   events.size(), topic, partition));
            for (KafkaLogEvent event : events) {
              avroFileWriter.append(event);
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
