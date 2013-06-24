/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.tail;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.app.logging.FlowletLoggingContext;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.LoggingConfiguration;
import com.continuuity.common.logging.logback.kafka.KafkaTopic;
import com.continuuity.common.logging.logback.kafka.LoggingEventSerializer;
import com.continuuity.logging.filter.Filter;
import com.continuuity.logging.filter.LogFilterGenerator;
import com.continuuity.logging.kafka.KafkaConsumer;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Tails logs by reading the log messages from Kafka.
 */
@SuppressWarnings("FieldCanBeLocal")
public final class KafkaLogTail implements LogTail {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaLogTail.class);

  private static final int WINDOW_SIZE = 1000;
  private final List<LoggingConfiguration.KafkaHost> seedBrokers;
  private final String topic;
  private final int numPartitions;
  private final LoggingEventSerializer serializer;

  private final int kafkaTailFetchTimeoutMs = 300;

  /**
   * Creates a KafkaLogTail object.
   * @param configuration configuration object containing Kafka seed brokers and number of Kafka partitions for log
   *                      topic.
   */
  @Inject
  public KafkaLogTail(@Assisted CConfiguration configuration) {
    try {
      this.seedBrokers = LoggingConfiguration.getKafkaSeedBrokers(
        configuration.get(LoggingConfiguration.KAFKA_SEED_BROKERS));
      Preconditions.checkArgument(!this.seedBrokers.isEmpty(), "Kafka seed brokers list is empty!");

      this.topic = KafkaTopic.getTopic();
      Preconditions.checkArgument(!this.topic.isEmpty(), "Kafka topic is emtpty!");

      this.numPartitions = configuration.getInt(LoggingConfiguration.NUM_PARTITIONS, -1);
      Preconditions.checkArgument(this.numPartitions > 0,
                                  "numPartitions should be greater than 0. Got numPartitions=%s", this.numPartitions);

      this.serializer = new LoggingEventSerializer();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void tailFlowLog(String accountId, String applicationId, String flowId, long fromTimeMs, int maxEvents,
                          Callback callback) {
    FlowletLoggingContext flowletLoggingContext = new FlowletLoggingContext(accountId, applicationId, flowId, "");
    Filter logFilter = LogFilterGenerator.createTailFilter(accountId, applicationId, flowId);
    tailLog(flowletLoggingContext.getLogPartition().hashCode() % numPartitions,
            fromTimeMs, maxEvents, logFilter, callback);
  }

  /**
   * Internal method to tail log events from a partition.
   * @param partition partition to read.
   * @param maxEvents max number of log events to return.
   * @param callback Callback interface to receive logging event.
   */
  private void tailLog(int partition, long fromTimeMs, int maxEvents, final Filter logFilter,
                       final Callback callback) {
    KafkaConsumer kafkaConsumer = new KafkaConsumer(seedBrokers, topic, partition, kafkaTailFetchTimeoutMs);

    try {
      final long earliestOffset = kafkaConsumer.fetchOffset(KafkaConsumer.Offset.EARLIEST);
      long endWindow = kafkaConsumer.fetchOffset(KafkaConsumer.Offset.LATEST);
      int numEvents = 0;

      while (numEvents < maxEvents) {
        int currentSize = maxEvents - numEvents > WINDOW_SIZE ? WINDOW_SIZE : maxEvents - numEvents;
        long beginWindow = endWindow - currentSize;
        if (beginWindow < earliestOffset) {
          beginWindow = earliestOffset;
        }

        if (beginWindow >= endWindow) {
          break;
        }

        TailCallback tailCallback = new TailCallback(logFilter, fromTimeMs, serializer, callback);
        int count = kafkaConsumer.fetchMessages(beginWindow, tailCallback);

        if (count == 0) {
          break;
        }

        numEvents += tailCallback.getNumEvents();
        endWindow = tailCallback.getFirstOffset();
      }
    } finally {
      try {
        kafkaConsumer.close();
      } catch (IOException e) {
        LOG.error(String.format("Caught exception when closing KafkaConsumer for topic %s, partition %d",
                                topic, partition), e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  private static class TailCallback implements com.continuuity.logging.kafka.Callback {
    private final Filter logFilter;
    private final long fromTimeMs;
    private final LoggingEventSerializer serializer;
    private final Callback callback;
    private int numEvents = 0;
    private long firstOffset = -1;

    private TailCallback(Filter logFilter, long fromTimeMs, LoggingEventSerializer serializer, Callback callback) {
      this.logFilter = logFilter;
      this.fromTimeMs = fromTimeMs;
      this.serializer = serializer;
      this.callback = callback;
    }

    @Override
    public void handle(long offset, ByteBuffer msgBuffer) {
      ILoggingEvent event = serializer.fromBytes(msgBuffer);
      if (event.getTimeStamp() >= fromTimeMs && logFilter.match(event)) {
        callback.handle(event);
        ++numEvents;
      }

      if (firstOffset == -1) {
        firstOffset = offset;
      }
    }

    public int getNumEvents() {
      return numEvents;
    }

    public long getFirstOffset() {
      return firstOffset;
    }
  }
}
