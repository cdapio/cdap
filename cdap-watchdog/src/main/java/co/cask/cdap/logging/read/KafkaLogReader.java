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
  public void getLogNext(final LoggingContext loggingContext, final LogOffset fromOffset, final int maxEvents,
                         final Filter filter, final Callback callback) {
    if (fromOffset.getKafkaOffset() < 0) {
      getLogPrev(loggingContext, fromOffset, maxEvents, filter, callback);
      return;
    }

    int partition = partitioner.partition(loggingContext.getLogPartition(), -1);

    callback.init();

    KafkaConsumer kafkaConsumer = new KafkaConsumer(seedBrokers, topic, partition, KAFKA_FETCH_TIMEOUT_MS);
    try {
      Filter logFilter = new AndFilter(ImmutableList.of(LoggingContextHelper.createFilter(loggingContext),
                                                        filter));

      long latestOffset = kafkaConsumer.fetchOffset(KafkaConsumer.Offset.LATEST);
      long startOffset = fromOffset.getKafkaOffset() + 1;

      if (startOffset >= latestOffset) {
        // At end of events, nothing to return
        return;
      }

      fetchLogEvents(kafkaConsumer, logFilter, startOffset, latestOffset, maxEvents, callback);
    } catch (Throwable e) {
      LOG.error("Got exception: ", e);
      throw  Throwables.propagate(e);
    } finally {
      try {
        try {
          callback.close();
        } finally {
          kafkaConsumer.close();
        }
      } catch (IOException e) {
        LOG.error(String.format("Caught exception when closing KafkaConsumer for topic %s, partition %d",
                                topic, partition), e);
      }
    }
  }

  @Override
  public void getLogPrev(final LoggingContext loggingContext, final LogOffset fromOffset, final int maxEvents,
                         final Filter filter, final Callback callback) {
    int partition = partitioner.partition(loggingContext.getLogPartition(), -1);

    callback.init();

    KafkaConsumer kafkaConsumer = new KafkaConsumer(seedBrokers, topic, partition, KAFKA_FETCH_TIMEOUT_MS);
    try {
      Filter logFilter = new AndFilter(ImmutableList.of(LoggingContextHelper.createFilter(loggingContext),
                                                        filter));

      long latestOffset = kafkaConsumer.fetchOffset(KafkaConsumer.Offset.LATEST);
      long earliestOffset = kafkaConsumer.fetchOffset(KafkaConsumer.Offset.EARLIEST);
      long stopOffset;
      long startOffset;

      if (fromOffset.getKafkaOffset() < 0)  {
        stopOffset = latestOffset;
      } else {
        stopOffset = fromOffset.getKafkaOffset();
      }
      startOffset = stopOffset - maxEvents;

      if (startOffset < earliestOffset) {
        startOffset = earliestOffset;
      }

      if (startOffset >= stopOffset || startOffset >= latestOffset) {
        // At end of kafka events, nothing to return
        return;
      }

      // Events between startOffset and stopOffset may not have the required logs we are looking for,
      // we'll need to return at least 1 log offset for next getLogPrev call to work.
      int fetchCount = 0;
      while (fetchCount == 0) {
        fetchCount = fetchLogEvents(kafkaConsumer, logFilter, startOffset, stopOffset, maxEvents, callback);
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
        try {
          callback.close();
        } finally {
          kafkaConsumer.close();
        }
      } catch (IOException e) {
        LOG.error(String.format("Caught exception when closing KafkaConsumer for topic %s, partition %d",
                                topic, partition), e);
      }
    }
  }

  @Override
  public void getLog(final LoggingContext loggingContext, final long fromTimeMs, final long toTimeMs,
                     final Filter filter, final Callback callback) {
    throw new UnsupportedOperationException("Getting logs by time is not supported by "
                                              + KafkaLogReader.class.getSimpleName());
  }

  private int fetchLogEvents(KafkaConsumer kafkaConsumer, Filter logFilter, long startOffset, long stopOffset,
                             int maxEvents, Callback callback) {
    KafkaCallback kafkaCallback = new KafkaCallback(logFilter, serializer, stopOffset, maxEvents, callback);

    while (kafkaCallback.getCount() < maxEvents && startOffset < stopOffset) {
      kafkaConsumer.fetchMessages(startOffset, kafkaCallback);
      long lastOffset = kafkaCallback.getLastOffset();

      // No more Kafka messages
      if (lastOffset == -1) {
        break;
      }
      startOffset = kafkaCallback.getLastOffset() + 1;
    }

    return kafkaCallback.getCount();
  }

  private static class KafkaCallback implements co.cask.cdap.logging.kafka.Callback {
    private final Filter logFilter;
    private final LoggingEventSerializer serializer;
    private final long stopOffset;
    private final int maxEvents;
    private final Callback callback;
    private long lastOffset = -1;
    private int count = 0;

    private KafkaCallback(Filter logFilter, LoggingEventSerializer serializer, long stopOffset, int maxEvents,
                          Callback callback) {
      this.logFilter = logFilter;
      this.serializer = serializer;
      this.stopOffset = stopOffset;
      this.maxEvents = maxEvents;
      this.callback = callback;
    }

    @Override
    public void handle(long offset, ByteBuffer msgBuffer) {
      ILoggingEvent event = serializer.fromBytes(msgBuffer);
      if (offset < stopOffset && count < maxEvents && logFilter.match(event)) {
        ++count;
        callback.handle(new LogEvent(event, new LogOffset(offset, event.getTimeStamp())));
      }
      lastOffset = offset;
    }

    public long getLastOffset() {
      return lastOffset;
    }

    public int getCount() {
      return count;
    }
  }
}
