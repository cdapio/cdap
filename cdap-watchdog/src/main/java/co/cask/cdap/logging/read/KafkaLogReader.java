/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.logging.read;

import ch.qos.logback.classic.spi.ILoggingEvent;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.appender.kafka.KafkaTopic;
import co.cask.cdap.logging.appender.kafka.LoggingEventSerializer;
import co.cask.cdap.logging.appender.kafka.StringPartitioner;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.filter.AndFilter;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.kafka.KafkaConsumer;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Reads log events stored in Kafka.
 */
public class KafkaLogReader implements LogReader {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaLogReader.class);
  private static final int KAFKA_FETCH_TIMEOUT_MS = 30000;
  // Maximum events to read from Kafka for any call
  private static final int MAX_READ_EVENTS_KAFKA = 10000;

  private final List<LoggingConfiguration.KafkaHost> seedBrokers;
  private final String topic;
  private final LoggingEventSerializer serializer;
  private final StringPartitioner partitioner;

  /**
   * Creates a Kafka log reader object.
   * @param cConfig configuration object containing Kafka seed brokers and number of Kafka partitions for log topic.
   */
  @Inject
  public KafkaLogReader(CConfiguration cConfig, StringPartitioner partitioner) {
    try {
      this.seedBrokers = LoggingConfiguration.getKafkaSeedBrokers(
        cConfig.get(LoggingConfiguration.KAFKA_SEED_BROKERS));
      Preconditions.checkArgument(!this.seedBrokers.isEmpty(), "Kafka seed brokers list is empty!");

      this.topic = KafkaTopic.getTopic();
      Preconditions.checkArgument(!this.topic.isEmpty(), "Kafka topic is emtpty!");

      this.partitioner = partitioner;
      this.serializer = new LoggingEventSerializer();

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void getLogNext(LoggingContext loggingContext, ReadRange readRange, int maxEvents,
                         Filter filter, Callback callback) {
    if (readRange.getKafkaOffset() == ReadRange.LATEST.getKafkaOffset()) {
      getLogPrev(loggingContext, readRange, maxEvents, filter, callback);
      return;
    }

    int partition = partitioner.partition(loggingContext.getLogPartition(), -1);
    LOG.trace("Reading from kafka partiton {}", partition);

    callback.init();

    KafkaConsumer kafkaConsumer = new KafkaConsumer(seedBrokers, topic, partition, KAFKA_FETCH_TIMEOUT_MS);
    try {
      // If Kafka offset is not valid, then we might be rolling over from file while reading.
      // Try to get the offset corresponding to fromOffset.getTime()
      if (readRange.getKafkaOffset() == LogOffset.INVALID_KAFKA_OFFSET) {
        readRange = new ReadRange(readRange.getFromMillis(), readRange.getToMillis(),
                                  kafkaConsumer.fetchOffsetBefore(readRange.getFromMillis()));
      }

      Filter logFilter = new AndFilter(ImmutableList.of(LoggingContextHelper.createFilter(loggingContext),
                                                        filter));

      long latestOffset = kafkaConsumer.fetchOffsetBefore(KafkaConsumer.LATEST_OFFSET);
      long startOffset = readRange.getKafkaOffset() + 1;

      LOG.trace("Using startOffset={}, latestOffset={}, readRange={}", startOffset, latestOffset, readRange);
      if (startOffset >= latestOffset) {
        // At end of events, nothing to return
        return;
      }

      KafkaCallback kafkaCallback = new KafkaCallback(logFilter, serializer, latestOffset, maxEvents, callback,
                                                      readRange.getFromMillis());

      fetchLogEvents(kafkaConsumer, kafkaCallback, startOffset, latestOffset, maxEvents, readRange);
    } catch (Throwable e) {
      LOG.error("Got exception: ", e);
      throw  Throwables.propagate(e);
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
  public void getLogPrev(LoggingContext loggingContext, ReadRange readRange, int maxEvents,
                         Filter filter, Callback callback) {
    if (readRange.getKafkaOffset() == LogOffset.INVALID_KAFKA_OFFSET) {
      readRange = new ReadRange(readRange.getFromMillis(), readRange.getToMillis(), ReadRange.LATEST.getKafkaOffset());
    }

    int partition = partitioner.partition(loggingContext.getLogPartition(), -1);
    LOG.trace("Reading from kafka partiton {}", partition);

    callback.init();

    KafkaConsumer kafkaConsumer = new KafkaConsumer(seedBrokers, topic, partition, KAFKA_FETCH_TIMEOUT_MS);
    try {
      Filter logFilter = new AndFilter(ImmutableList.of(LoggingContextHelper.createFilter(loggingContext),
                                                        filter));

      long latestOffset = kafkaConsumer.fetchOffsetBefore(KafkaConsumer.LATEST_OFFSET);
      long earliestOffset = kafkaConsumer.fetchOffsetBefore(KafkaConsumer.EARLIEST_OFFSET);
      long stopOffset;
      long startOffset;

      if (readRange.getKafkaOffset() < 0)  {
        stopOffset = latestOffset;
      } else {
        stopOffset = readRange.getKafkaOffset();
      }
      startOffset = stopOffset - maxEvents;

      if (startOffset < earliestOffset) {
        startOffset = earliestOffset;
      }

      LOG.trace("Using startOffset={}, latestOffset={}, readRange={}", startOffset, latestOffset, readRange);
      if (startOffset >= stopOffset || startOffset >= latestOffset) {
        // At end of kafka events, nothing to return
        return;
      }

      KafkaCallback kafkaCallback = new KafkaCallback(logFilter, serializer, stopOffset, maxEvents, callback,
                                                      readRange.getFromMillis());

      // Events between startOffset and stopOffset may not have the required logs we are looking for,
      // we'll need to return at least 1 log offset for next getLogPrev call to work.
      int fetchCount = 0;
      while (fetchCount == 0 && kafkaCallback.getEventsRead() <= MAX_READ_EVENTS_KAFKA) {
        fetchCount = fetchLogEvents(kafkaConsumer, kafkaCallback, startOffset, stopOffset, maxEvents, readRange);
        stopOffset = startOffset;
        if (stopOffset <= earliestOffset) {
          // Truly no log messages found.
          break;
        }

        startOffset = stopOffset - maxEvents;
        if (startOffset < earliestOffset) {
          startOffset = earliestOffset;
        }
      }
    } catch (Throwable e) {
      LOG.error("Got exception: ", e);
      throw  Throwables.propagate(e);
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
  public void getLog(LoggingContext loggingContext, long fromTimeMs, long toTimeMs,
                     Filter filter, Callback callback) {
    throw new UnsupportedOperationException("Getting logs by time is not supported by "
                                              + KafkaLogReader.class.getSimpleName());
  }

  private int fetchLogEvents(KafkaConsumer kafkaConsumer, KafkaCallback kafkaCallback,
                             long startOffset, long stopOffset, int maxEvents, ReadRange readRange) {
    while (kafkaCallback.getEventsMatched() < maxEvents && startOffset < stopOffset) {
      kafkaConsumer.fetchMessages(startOffset, kafkaCallback);
      LogOffset lastOffset = kafkaCallback.getLastOffset();
      LogOffset firstOffset = kafkaCallback.getFirstOffset();

      // No more Kafka messages
      if (lastOffset == null) {
        break;
      }
      // If out of range, break
      if (firstOffset.getTime() < readRange.getFromMillis() || lastOffset.getTime() > readRange.getToMillis()) {
        break;
      }
      // If read more than MAX_READ_EVENTS_KAFKA, break
      if (kafkaCallback.getEventsRead() > MAX_READ_EVENTS_KAFKA) {
        break;
      }
      startOffset = kafkaCallback.getLastOffset().getKafkaOffset() + 1;
    }

    return kafkaCallback.getEventsMatched();
  }

  private static class KafkaCallback implements co.cask.cdap.logging.kafka.Callback {
    private final Filter logFilter;
    private final LoggingEventSerializer serializer;
    private final long stopOffset;
    private final int maxEvents;
    private final Callback callback;
    private final long fromTimeMs;

    private LogOffset firstOffset;
    private LogOffset lastOffset;
    private int eventsMatched = 0;
    private int eventsRead = 0;

    private KafkaCallback(Filter logFilter, LoggingEventSerializer serializer, long stopOffset, int maxEvents,
                          Callback callback, long fromTimeMs) {
      this.logFilter = logFilter;
      this.serializer = serializer;
      this.stopOffset = stopOffset;
      this.maxEvents = maxEvents;
      this.callback = callback;
      this.fromTimeMs = fromTimeMs;
    }

    @Override
    public void handle(long offset, ByteBuffer msgBuffer) {
      ++eventsRead;
      ILoggingEvent event = serializer.fromBytes(msgBuffer);
      LogOffset logOffset = new LogOffset(offset, event.getTimeStamp());

      if (offset < stopOffset && eventsMatched < maxEvents && logFilter.match(event) &&
        event.getTimeStamp() > fromTimeMs) {
        ++eventsMatched;
        callback.handle(new LogEvent(event, logOffset));
      }

      if (firstOffset == null) {
        firstOffset = logOffset;
      }
      lastOffset = logOffset;
    }

    public LogOffset getFirstOffset() {
      return firstOffset;
    }

    public LogOffset getLastOffset() {
      return lastOffset;
    }

    public int getEventsMatched() {
      return eventsMatched;
    }

    public int getEventsRead() {
      return eventsRead;
    }
  }
}
