/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.logging.save;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.RootLocationFactory;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.appender.kafka.LoggingEventSerializer;
import co.cask.cdap.logging.kafka.KafkaLogEvent;
import co.cask.cdap.logging.write.AvroFileWriter;
import co.cask.cdap.logging.write.FileMetaDataManager;
import co.cask.cdap.logging.write.LogCleanup;
import co.cask.cdap.logging.write.LogFileWriter;
import co.cask.cdap.proto.Id;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.RowSortedTable;
import com.google.common.collect.TreeBasedTable;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Plugin that writes the log data.
 */
public class KafkaLogWriterPlugin extends AbstractKafkaLogProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaLogWriterPlugin.class);

  public static final int CHECKPOINT_ROW_KEY_PREFIX = 100;

  private static final long SLEEP_TIME_MS = 100;

  private final String logBaseDir;
  private final LogFileWriter<KafkaLogEvent> logFileWriter;
  // Table structure - <event time bucket, log context, event arrival time bucket, log event>
  private final RowSortedTable<Long, String, Map.Entry<Long, List<KafkaLogEvent>>> messageTable;
  private final long eventBucketIntervalMs;
  private final int logCleanupIntervalMins;
  private final long maxNumberOfBucketsInTable;
  private final LogCleanup logCleanup;
  private final CheckpointManager checkpointManager;
  private final long maxNumberOfEvents;
  private final MetricsContext metricsContext;

  private ListeningScheduledExecutorService scheduledExecutor;
  private CountDownLatch countDownLatch;
  private int partition;
  private String numBucketsMetric;

  KafkaLogWriterPlugin(CConfiguration cConf, FileMetaDataManager fileMetaDataManager,
                       CheckpointManagerFactory checkpointManagerFactory, RootLocationFactory rootLocationFactory,
                       NamespacedLocationFactory namespacedLocationFactory, Impersonator impersonator,
                       MetricsCollectionService metricsCollectionService) throws Exception {

    LoggingEventSerializer serializer = new LoggingEventSerializer();
    this.messageTable = TreeBasedTable.create();

    this.logBaseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR);
    Preconditions.checkNotNull(this.logBaseDir, "Log base dir cannot be null");
    LOG.debug(String.format("Log base dir is %s", this.logBaseDir));

    long retentionDurationDays = cConf.getLong(LoggingConfiguration.LOG_RETENTION_DURATION_DAYS,
                                               LoggingConfiguration.DEFAULT_LOG_RETENTION_DURATION_DAYS);
    Preconditions.checkArgument(retentionDurationDays > 0,
                                "Log file retention duration is invalid: %s", retentionDurationDays);

    // Keep max file size a little less than the default HDFS block size (128 MB)
    long maxLogFileSizeBytes = cConf.getLong(LoggingConfiguration.LOG_MAX_FILE_SIZE_BYTES, 100 * 1000 * 1000);
    Preconditions.checkArgument(maxLogFileSizeBytes > 0,
                                "Max log file size is invalid: %s", maxLogFileSizeBytes);

    // Sync interval should be around 10 times smaller than file size as we use sync points to navigate
    int syncIntervalBytes = cConf.getInt(LoggingConfiguration.LOG_FILE_SYNC_INTERVAL_BYTES, 10 * 1000 * 1000);
    Preconditions.checkArgument(syncIntervalBytes > 0,
                                "Log file sync interval is invalid: %s", syncIntervalBytes);

    long checkpointIntervalMs = cConf.getLong(LoggingConfiguration.LOG_SAVER_CHECKPOINT_INTERVAL_MS,
                                              LoggingConfiguration.DEFAULT_LOG_SAVER_CHECKPOINT_INTERVAL_MS);
    Preconditions.checkArgument(checkpointIntervalMs > 0,
                                "Checkpoint interval is invalid: %s", checkpointIntervalMs);

    long maxFileLifetimeMs = cConf.getLong(LoggingConfiguration.LOG_SAVER_MAX_FILE_LIFETIME,
                                           LoggingConfiguration.DEFAULT_LOG_SAVER_MAX_FILE_LIFETIME_MS);
    Preconditions.checkArgument(maxFileLifetimeMs > 0,
                                "Max file lifetime is invalid: %s", maxFileLifetimeMs);

    if (cConf.get(LoggingConfiguration.LOG_SAVER_INACTIVE_FILE_INTERVAL_MS) != null) {
      LOG.warn("Parameter '{}' is no longer supported. Instead, use '{}'.",
               LoggingConfiguration.LOG_SAVER_INACTIVE_FILE_INTERVAL_MS,
               LoggingConfiguration.LOG_SAVER_MAX_FILE_LIFETIME);
    }

    this.eventBucketIntervalMs = cConf.getLong(LoggingConfiguration.LOG_SAVER_EVENT_BUCKET_INTERVAL_MS,
                                               LoggingConfiguration.DEFAULT_LOG_SAVER_EVENT_BUCKET_INTERVAL_MS);
    Preconditions.checkArgument(this.eventBucketIntervalMs > 0,
                                "Event bucket interval is invalid: %s", this.eventBucketIntervalMs);

    this.maxNumberOfEvents = cConf.getLong(LoggingConfiguration.LOG_SAVER_MAXIMUM_INMEMORY_EVENTS,
                                           LoggingConfiguration.DEFAULT_LOG_SAVER_MAXIMUM_INMEMORY_EVENTS);
    Preconditions.checkArgument(this.maxNumberOfEvents > 0,
                                "Max number of events is invalid: %s", this.maxNumberOfEvents);

    this.metricsContext = metricsCollectionService.getContext(
      ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, Id.Namespace.SYSTEM.getId(),
                      Constants.Metrics.Tag.COMPONENT, Constants.Service.LOGSAVER));

    this.maxNumberOfBucketsInTable = cConf.getLong
      (LoggingConfiguration.LOG_SAVER_MAXIMUM_INMEMORY_EVENT_BUCKETS,
       LoggingConfiguration.DEFAULT_LOG_SAVER_MAXIMUM_INMEMORY_EVENT_BUCKETS);
    Preconditions.checkArgument(this.maxNumberOfBucketsInTable > 0,
                                "Maximum number of event buckets in memory is invalid: %s",
                                this.maxNumberOfBucketsInTable);

    long topicCreationSleepMs = cConf.getLong(LoggingConfiguration.LOG_SAVER_TOPIC_WAIT_SLEEP_MS,
                                              LoggingConfiguration.DEFAULT_LOG_SAVER_TOPIC_WAIT_SLEEP_MS);
    Preconditions.checkArgument(topicCreationSleepMs > 0,
                                "Topic creation wait sleep is invalid: %s", topicCreationSleepMs);

    logCleanupIntervalMins = cConf.getInt(LoggingConfiguration.LOG_CLEANUP_RUN_INTERVAL_MINS,
                                          LoggingConfiguration.DEFAULT_LOG_CLEANUP_RUN_INTERVAL_MINS);
    Preconditions.checkArgument(logCleanupIntervalMins > 0,
                                "Log cleanup run interval is invalid: %s", logCleanupIntervalMins);

    AvroFileWriter avroFileWriter = new AvroFileWriter(fileMetaDataManager, namespacedLocationFactory, logBaseDir,
                                                       serializer.getAvroSchema(), maxLogFileSizeBytes,
                                                       syncIntervalBytes, maxFileLifetimeMs, impersonator);

    checkpointManager = checkpointManagerFactory.create(cConf.get(Constants.Logging.KAFKA_TOPIC),
                                                        CHECKPOINT_ROW_KEY_PREFIX);

    this.logFileWriter = new CheckpointingLogFileWriter(avroFileWriter, checkpointManager, checkpointIntervalMs);
    long retentionDurationMs = TimeUnit.MILLISECONDS.convert(retentionDurationDays, TimeUnit.DAYS);
    this.logCleanup = new LogCleanup(fileMetaDataManager, rootLocationFactory, retentionDurationMs, impersonator);
  }

  @Override
  public void init(int partition) throws Exception {
    this.partition = partition;
    numBucketsMetric = Constants.Metrics.Name.Log.NUM_BUCKETS + "." + partition;
    Checkpoint checkpoint = checkpointManager.getCheckpoint(partition);
    super.init(checkpoint);

    // We schedule clean up task if partition is zero, so that only one cleanup task gets scheduled
    if (partition == 0) {
      scheduledExecutor = MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(2,
        Threads.createDaemonThreadFactory("log-saver-log-processor-" + partition)));
      LOG.info("Scheduling cleanup task");
      scheduledExecutor.scheduleAtFixedRate(logCleanup, 10, logCleanupIntervalMins, TimeUnit.MINUTES);
    } else {
      scheduledExecutor = MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("log-saver-log-processor-" + partition)));
    }

    countDownLatch = new CountDownLatch(1);
    LogWriter logWriter = new LogWriter(logFileWriter, messageTable,
                                        eventBucketIntervalMs, maxNumberOfBucketsInTable, countDownLatch);
    scheduledExecutor.execute(logWriter);
  }

  @Override
  public void doProcess(Iterator<KafkaLogEvent> events) {
    // Get the timestamp from the first event to compute time bucket
    PeekingIterator<KafkaLogEvent> peekingIterator = Iterators.peekingIterator(events);
    KafkaLogEvent firstEvent = peekingIterator.peek();

    try {
      // Compute the bucket number for the first event
      long firstKey = firstEvent.getLogEvent().getTimeStamp() / eventBucketIntervalMs;

      // Sleep while we can add the entry
      while (true) {
        // Get the oldest bucket in the table
        long oldestBucketKey;
        int numBuckets;
        synchronized (messageTable) {
          SortedSet<Long> rowKeySet = messageTable.rowKeySet();
          numBuckets = rowKeySet.size();
          oldestBucketKey = numBuckets == 0 ? System.currentTimeMillis() / eventBucketIntervalMs : rowKeySet.first();
          long latestBucketKey = numBuckets == 0 ? oldestBucketKey : rowKeySet.last();

          // emitting number of buckets as metrics
          metricsContext.gauge(numBucketsMetric, numBuckets);

          // If the number of buckets in memory are less than maxBuckets or
          // if the current event falls in the bucket number which is in
          // the window [oldestBucketKey, oldestBucketKey+maxBuckets]
          // then we can add the event to message table
          // We try to limit in-memory buckets to prevent OOM.
          // Note that we can still have more than maxBuckets in memory if buckets are not consecutive, or
          // we get an event older than oldest bucket
          long messageTableTotalEvents = getMessageTableTotalEvents();
          if (numBuckets < maxNumberOfBucketsInTable || firstKey <= latestBucketKey) {
            // We will limit max number of events held in memory to prevent OOM. We do not add more events if we have
            // reached maxNumberOfEvents
            while (messageTableTotalEvents < maxNumberOfEvents && peekingIterator.hasNext()) {
              KafkaLogEvent event = peekingIterator.next();
              LoggingContext loggingContext = event.getLoggingContext();
              long key = event.getLogEvent().getTimeStamp() / eventBucketIntervalMs;

              Map.Entry<Long, List<KafkaLogEvent>> entry =
                messageTable.get(key, loggingContext.getLogPathFragment(logBaseDir));
              List<KafkaLogEvent> msgList;
              if (entry == null) {
                long eventArrivalBucketKey = System.currentTimeMillis() / eventBucketIntervalMs;
                msgList = Lists.newArrayList();
                messageTable.put(key, loggingContext.getLogPathFragment(logBaseDir),
                                 new AbstractMap.SimpleEntry<>(eventArrivalBucketKey, msgList));
              } else {
                msgList = messageTable.get(key, loggingContext.getLogPathFragment(logBaseDir)).getValue();
              }
              msgList.add(new KafkaLogEvent(event.getGenericRecord(), event.getLogEvent(), loggingContext,
                                            event.getPartition(), event.getNextOffset()));
              messageTableTotalEvents++;
            }
            break;
          }
        }

        // Cannot insert event into message table
        // since there are still maxNumberOfBucketsInTable buckets that need to be processed
        // sleep for the time duration till event falls in the window
        LOG.trace("key={}, oldestBucketKey={}, maxNumberOfBucketsInTable={}, buckets={}. Sleeping for {} ms.",
                  firstKey, oldestBucketKey, maxNumberOfBucketsInTable, numBuckets, SLEEP_TIME_MS);

        if (countDownLatch.await(SLEEP_TIME_MS, TimeUnit.MILLISECONDS)) {
          // if count down occurred return
          LOG.debug("Returning since callback is cancelled");
          return;
        }
      }
    } catch (Throwable th) {
      LOG.warn("Exception while processing message with nextOffset {}. Skipping it.", firstEvent.getNextOffset(), th);
    }
  }

  private long getMessageTableTotalEvents() {
    long totalEvents = 0;

    for (Map.Entry<Long, List<KafkaLogEvent>> eventArrivalTimeBucket : messageTable.values()) {
      totalEvents += eventArrivalTimeBucket.getValue().size();
    }

    return totalEvents;
  }


  @Override
  public void stop() {
    try {
      LOG.info("Stopping log writer plugin for partition {}", partition);
      if (countDownLatch != null) {
        countDownLatch.countDown();
      }

      if (scheduledExecutor != null) {
        scheduledExecutor.shutdown();
        if (!scheduledExecutor.awaitTermination(5, TimeUnit.MINUTES)) {
          scheduledExecutor.shutdownNow();
        }
      }

      logFileWriter.flush();
      logFileWriter.close();

    } catch (Exception e) {
      LOG.error("Caught exception while closing logWriter {}", e.getMessage(), e);
    }
    messageTable.clear();
  }

  @Override
  public Checkpoint getCheckpoint() {
    try {
      return checkpointManager.getCheckpoint(partition);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @VisibleForTesting
  CheckpointManager getCheckPointManager() {
    return this.checkpointManager;
  }
}
