package com.continuuity.logging.save;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.logging.logback.kafka.LoggingEventSerializer;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.logging.LoggingContextLookup;
import com.continuuity.logging.kafka.KafkaConsumer;
import com.continuuity.logging.kafka.KafkaLogEvent;
import com.continuuity.logging.kafka.KafkaMessage;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
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
public class LogSaver extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(LogSaver.class);

  private List<ImmutablePair<String, Integer>> seedBrokers;
  private String topic;
  private int partition;
  private LoggingEventSerializer serializer;

  private final QueueManager queueManager;
  private final AvroFileWriter avroFileWriter;
  private final long kafkaErrorSleepMs;
  private final long kafkaEmptySleepMs;
  private final int kafkaSaveFetchTimeoutMs = 1000;
  private final int syncIntervalBytes = 1024 * 1024;
  private final long maxLogFileSize = 100 * 1024 * 1024;

  private final int numThreads = 2;

  public LogSaver() throws IOException, URISyntaxException {
    this.seedBrokers = Lists.newArrayList(new ImmutablePair<String, Integer>("localhost", 9094));
    this.topic = "LOG_MESSAGES";
    this.partition = 0;
    this.serializer = new LoggingEventSerializer();


    this.queueManager = new QueueManager(3000);
    this.avroFileWriter = new AvroFileWriter(FileSystem.get(new Configuration()), "/tmp/logs", serializer.getSchema(),
                                             maxLogFileSize, syncIntervalBytes);
    this.kafkaErrorSleepMs = 2000;
    this.kafkaEmptySleepMs = 2000;
  }

  @Override
  protected void run() throws Exception {
    LOG.info("Starting LogSaver...");
    ListeningExecutorService listeningExecutorService =
      MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numThreads));
    List<ListenableFuture<?>> futures = Lists.newArrayList();

    ListenableFuture<?> future = listeningExecutorService.submit(new LogCollector());
    futures.add(future);

    future = listeningExecutorService.submit(new LogWriter());
    futures.add(future);

    // Wait for tasks to complete
    Future<?> compositeFuture = Futures.allAsList(futures);
    compositeFuture.get();

    LOG.info("Stopping LogSaver...");

    listeningExecutorService.shutdownNow();
  }

  class LogCollector implements Runnable {
    @Override
    public void run() {
      KafkaConsumer kafkaConsumer = null;
      try {
        kafkaConsumer = new KafkaConsumer(seedBrokers, topic, partition, kafkaSaveFetchTimeoutMs);
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
          if (kafkaConsumer != null) {
            kafkaConsumer.close();
          }
        } catch (IOException e) {
          LOG.error(String.format("Caught exception while closing KafkaConsumer for topic %s, partition %d:",
                                  topic, partition), e);
        }
      }
    }
  }

  class LogWriter implements Runnable {
    @Override
    public void run() {
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
