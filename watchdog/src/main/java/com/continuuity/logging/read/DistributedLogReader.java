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
import com.continuuity.logging.filter.AndFilter;
import com.continuuity.logging.filter.Filter;
import com.continuuity.logging.kafka.KafkaConsumer;
import com.continuuity.logging.save.FileMetaDataManager;
import com.continuuity.logging.serialize.LogSchema;
import com.continuuity.weave.common.Threads;
import com.google.common.base.Preconditions;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Reads logs in a distributed setup.
 */
@SuppressWarnings("FieldCanBeLocal")
public final class DistributedLogReader implements LogReader {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedLogReader.class);

  private static final int MAX_THREAD_POOL_SIZE = 20;

  private final List<LoggingConfiguration.KafkaHost> seedBrokers;
  private final String topic;
  private final int numPartitions;
  private final LoggingEventSerializer serializer;
  private final FileMetaDataManager fileMetaDataManager;
  private final Configuration hConfig;
  private final Schema schema;
  private final ExecutorService executor;

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

    // Thread pool of size max MAX_THREAD_POOL_SIZE.
    // 60 seconds wait time before killing idle threads.
    // Keep no idle threads more than 60 seconds.
    // If max thread pool size reached, reject the new coming
    executor =
      new ThreadPoolExecutor(0, MAX_THREAD_POOL_SIZE,
                             60L, TimeUnit.SECONDS,
                             new SynchronousQueue<Runnable>(),
                             Threads.createDaemonThreadFactory("dist-log-reader-%d"),
                             new ThreadPoolExecutor.DiscardPolicy());
  }

  @Override
  public Future<?> getLogNext(final LoggingContext loggingContext, final long fromOffset, final int maxEvents,
                              final Filter filter, final Callback callback) {
    return executor.submit(
      new Runnable() {
        @Override
        public void run() {
          Filter logFilter = new AndFilter(ImmutableList.of(LoggingContextHelper.createFilter(loggingContext), filter));
          int partition = MD5Hash.digest(loggingContext.getLogPartition()).hashCode() % numPartitions;

          KafkaConsumer kafkaConsumer = new KafkaConsumer(seedBrokers, topic, partition, kafkaTailFetchTimeoutMs);

          try {
            long latestOffset = kafkaConsumer.fetchOffset(KafkaConsumer.Offset.LATEST);
            long startOffset = fromOffset + 1;
            if (fromOffset < 0 || startOffset >= latestOffset) {
              startOffset = latestOffset - maxEvents - 1;
            }

            callback.init();
            fetchLogEvents(kafkaConsumer, logFilter, startOffset, latestOffset, maxEvents, callback);
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
      }
    );
  }

  @Override
  public Future<?> getLogPrev(final LoggingContext loggingContext, final long fromOffset, final int maxEvents,
                              final Filter filter, final Callback callback) {
    return executor.submit(
      new Runnable() {
        @Override
        public void run() {
          Filter logFilter = new AndFilter(ImmutableList.of(LoggingContextHelper.createFilter(loggingContext), filter));
          int partition = MD5Hash.digest(loggingContext.getLogPartition()).hashCode() % numPartitions;

          KafkaConsumer kafkaConsumer = new KafkaConsumer(seedBrokers, topic, partition, kafkaTailFetchTimeoutMs);

          try {
            long latestOffset = kafkaConsumer.fetchOffset(KafkaConsumer.Offset.LATEST);
            long startOffset = fromOffset - maxEvents;
            if (fromOffset < 0 || startOffset >= latestOffset)  {
              startOffset = latestOffset - maxEvents - 1;
            }

            callback.init();
            fetchLogEvents(kafkaConsumer, logFilter, startOffset, latestOffset, maxEvents, callback);
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
      }
    );
  }


  @Override
  public Future<?> getLog(final LoggingContext loggingContext, final long fromTimeMs, final long toTimeMs,
                          final Filter filter, final Callback callback) {
    return executor.submit(
      new Runnable() {
        @Override
        public void run() {
          Filter logFilter = new AndFilter(ImmutableList.of(LoggingContextHelper.createFilter(loggingContext), filter));

          try {
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

            callback.init();
            AvroFileLogReader avroFileLogReader = new AvroFileLogReader(hConfig, schema);
            for (Path file : files) {
              avroFileLogReader.readLog(file, logFilter, fromTimeMs, toTimeMs, Integer.MAX_VALUE, callback);
            }
          } catch (OperationException e) {
            throw  Throwables.propagate(e);
          } finally {
            callback.close();
          }
        }
      }
    );
  }

  @Override
  public void close() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  private void fetchLogEvents(KafkaConsumer kafkaConsumer, Filter logFilter, long startOffset, long latestOffset,
                              int maxEvents, Callback callback) {
    KafkaCallback kafkaCallback = new KafkaCallback(logFilter, serializer, maxEvents, callback);

    long earliestOffset = kafkaConsumer.fetchOffset(KafkaConsumer.Offset.EARLIEST);
    if (startOffset < earliestOffset) {
      startOffset = earliestOffset;
    }

    while (kafkaCallback.getCount() < maxEvents && startOffset < latestOffset) {
      kafkaConsumer.fetchMessages(startOffset, kafkaCallback);
      long lastOffset = kafkaCallback.getLastOffset();

      // No more Kafka messages
      if (lastOffset == -1) {
        break;
      }
      startOffset = kafkaCallback.getLastOffset() + 1;
    }
  }

  private static class KafkaCallback implements com.continuuity.logging.kafka.Callback {
    private final Filter logFilter;
    private final LoggingEventSerializer serializer;
    private final int maxEvents;
    private final Callback callback;
    private long lastOffset;
    private int count = 0;

    private KafkaCallback(Filter logFilter, LoggingEventSerializer serializer, int maxEvents, Callback callback) {
      this.logFilter = logFilter;
      this.serializer = serializer;
      this.maxEvents = maxEvents;
      this.callback = callback;
    }

    @Override
    public void handle(long offset, ByteBuffer msgBuffer) {
      ++count;
      ILoggingEvent event = serializer.fromBytes(msgBuffer);
      if (count <= maxEvents && logFilter.match(event)) {
        callback.handle(new LogEvent(event, offset));
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
