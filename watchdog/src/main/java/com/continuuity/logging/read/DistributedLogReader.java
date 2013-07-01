/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.read;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.logging.LoggingConfiguration;
import com.continuuity.logging.appender.kafka.KafkaTopic;
import com.continuuity.logging.appender.kafka.LoggingEventSerializer;
import com.continuuity.logging.context.LoggingContextHelper;
import com.continuuity.logging.filter.Filter;
import com.continuuity.logging.kafka.KafkaConsumer;
import com.continuuity.logging.save.FileMetaDataManager;
import com.continuuity.logging.serialize.LogSchema;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MD5Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 * Reads logs in a distributed setup.
 */
@SuppressWarnings("FieldCanBeLocal")
public final class DistributedLogReader implements LogReader {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedLogReader.class);

  private final List<LoggingConfiguration.KafkaHost> seedBrokers;
  private final String topic;
  private final int numPartitions;
  private final LoggingEventSerializer serializer;
  private final FileMetaDataManager fileMetaDataManager;
  private final Configuration hConfig;
  private final Schema schema;

  private final int kafkaTailFetchTimeoutMs = 300;

  /**
   * Creates a DistributedLogReader object.
   * @param cConfig configuration object containing Kafka seed brokers and number of Kafka partitions for log topic.
   */
  @Inject
  public DistributedLogReader(OperationExecutor opex, CConfiguration cConfig, Configuration hConfig) {
    try {
      this.seedBrokers = LoggingConfiguration.getKafkaSeedBrokers(
        cConfig.get(LoggingConfiguration.KAFKA_SEED_BROKERS));
      Preconditions.checkArgument(!this.seedBrokers.isEmpty(), "Kafka seed brokers list is empty!");

      this.topic = KafkaTopic.getTopic();
      Preconditions.checkArgument(!this.topic.isEmpty(), "Kafka topic is emtpty!");

      this.numPartitions = cConfig.getInt(LoggingConfiguration.NUM_PARTITIONS, -1);
      Preconditions.checkArgument(this.numPartitions > 0,
                                  "numPartitions should be greater than 0. Got numPartitions=%s", this.numPartitions);

      this.serializer = new LoggingEventSerializer();

      String account = cConfig.get(LoggingConfiguration.LOG_RUN_ACCOUNT);
      Preconditions.checkNotNull(account, "Account cannot be null");
      this.fileMetaDataManager = new FileMetaDataManager(opex, new OperationContext(account),
                                                         LoggingConfiguration.LOG_META_DATA_TABLE);

      this.hConfig = hConfig;
      this.schema = new LogSchema().getAvroSchema();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Result getLogNext(LoggingContext loggingContext, String positionHintString, int maxEvents) {
    Filter logFilter = LoggingContextHelper.createFilter(loggingContext);
    PositionHint positionHint = new PositionHint(positionHintString);
    int partition = MD5Hash.digest(loggingContext.getLogPartition()).hashCode() % numPartitions;

    KafkaConsumer kafkaConsumer = new KafkaConsumer(seedBrokers, topic, partition, kafkaTailFetchTimeoutMs);

    try {
      long latestOffset = kafkaConsumer.fetchOffset(KafkaConsumer.Offset.LATEST);
      long startOffset = positionHint.isValid() ? positionHint.getNextOffset() : latestOffset - maxEvents;

      return fetchLogEvents(kafkaConsumer, logFilter, positionHint, startOffset, latestOffset, maxEvents);
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
  public Result getLogPrev(LoggingContext loggingContext, String positionHintString, int maxEvents) {
    Filter logFilter = LoggingContextHelper.createFilter(loggingContext);
    PositionHint positionHint = new PositionHint(positionHintString);
    int partition = MD5Hash.digest(loggingContext.getLogPartition()).hashCode() % numPartitions;

    KafkaConsumer kafkaConsumer = new KafkaConsumer(seedBrokers, topic, partition, kafkaTailFetchTimeoutMs);

    try {
      long latestOffset = kafkaConsumer.fetchOffset(KafkaConsumer.Offset.LATEST);
      long startOffset = positionHint.isValid() ? positionHint.getPrevOffset() - maxEvents : latestOffset - maxEvents;

      return fetchLogEvents(kafkaConsumer, logFilter, positionHint, startOffset, latestOffset, maxEvents);
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
                     com.continuuity.logging.read.Callback callback) {
    try {
      Filter logFilter = LoggingContextHelper.createFilter(loggingContext);

      SortedMap<Long, Path> sortedFiles = fileMetaDataManager.listFiles(loggingContext);
      Path prevFile = null;
      List<Path> files = Lists.newArrayListWithExpectedSize(sortedFiles.size());
      for (Map.Entry<Long, Path> entry : sortedFiles.entrySet()) {
        if (entry.getKey() >= fromTimeMs && entry.getKey() < toTimeMs && prevFile != null) {
          files.add(prevFile);
        }
        prevFile = entry.getValue();
      }

      if (prevFile != null) {
        files.add(prevFile);
      }

      AvroFileLogReader logReader = new AvroFileLogReader(hConfig, schema);
      for (Path file : files) {
        logReader.readLog(file, logFilter, fromTimeMs, toTimeMs, Integer.MAX_VALUE, callback);
      }
    } catch (OperationException e) {
      throw  Throwables.propagate(e);
    }
  }

  private Result fetchLogEvents(KafkaConsumer kafkaConsumer, Filter logFilter, PositionHint positionHint,
                                long startOffset, long latestOffset, int maxEvents) {
    Callback callback = new Callback(logFilter, serializer, maxEvents);

    long earliestOffset = kafkaConsumer.fetchOffset(KafkaConsumer.Offset.EARLIEST);
    if (startOffset < earliestOffset) {
      startOffset = earliestOffset;
    }

    while (callback.getEvents().size() < maxEvents && startOffset < latestOffset) {
      kafkaConsumer.fetchMessages(startOffset, callback);
      long lastOffset = callback.getLastOffset();

      // No more Kafka messages
      if (lastOffset == -1) {
        break;
      }
      startOffset = callback.getLastOffset() + 1;
    }

    if (callback.getEvents().isEmpty()) {
      return new Result(callback.getEvents(), positionHint.getPositionHintString(), positionHint.isValid());
    } else {
      return new Result(callback.getEvents(),
                        PositionHint.genPositionHint(startOffset - 1, startOffset + callback.getEvents().size()),
                        positionHint.isValid());
    }
  }

  private static class Callback implements com.continuuity.logging.kafka.Callback {
    private final Filter logFilter;
    private final LoggingEventSerializer serializer;
    private final int maxEvents;
    private final List<ILoggingEvent> events;
    private long lastOffset;

    private Callback(Filter logFilter, LoggingEventSerializer serializer, int maxEvents) {
      this.logFilter = logFilter;
      this.serializer = serializer;
      this.maxEvents = maxEvents;
      this.events = Lists.newArrayListWithExpectedSize(this.maxEvents);
    }

    @Override
    public void handle(long offset, ByteBuffer msgBuffer) {
      ILoggingEvent event = serializer.fromBytes(msgBuffer);
      if (events.size() <= maxEvents && logFilter.match(event)) {
        events.add(event);
      }
      lastOffset = offset;
    }

    public List<ILoggingEvent> getEvents() {
      return events;
    }

    public long getLastOffset() {
      return lastOffset;
    }
  }

  static class PositionHint {
    private static final String POS_HINT_PREFIX = "KAFKA";
    private static final int POS_HINT_NUM_FILEDS = 3;

    private String positionHintString;
    private long prevOffset = -1;
    private long nextOffset = -1;

    private static final Splitter SPLITTER = Splitter.on(':').limit(POS_HINT_NUM_FILEDS);

    public PositionHint(String positionHint) {
      this.positionHintString = positionHint;

      if (positionHint == null || positionHint.isEmpty()) {
        return;
      }

      List<String> splits = ImmutableList.copyOf(SPLITTER.split(positionHint));
      if (splits.size() != POS_HINT_NUM_FILEDS || !splits.get(0).equals(POS_HINT_PREFIX)) {
        return;
      }

      try {
        this.prevOffset = Long.parseLong(splits.get(1));
        this.nextOffset = Long.parseLong(splits.get(2));
      } catch (NumberFormatException e) {
        // Cannot parse position hint
        this.prevOffset = -1;
        this.nextOffset = -1;
      }
    }

    public String getPositionHintString() {
      return positionHintString;
    }

    public long getPrevOffset() {
      return prevOffset;
    }

    public long getNextOffset() {
      return nextOffset;
    }

    public boolean isValid() {
      return prevOffset > -1 && nextOffset > -1;
    }

    public static String genPositionHint(long prevOffset, long nextOffset) {
      if (prevOffset > -1 && nextOffset > -1) {
        return String.format("%s:%d:%d", POS_HINT_PREFIX, prevOffset, nextOffset);
      }
      return "";
    }
  }
}
