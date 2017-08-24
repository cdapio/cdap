/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.metrics.process;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.MetricValue;
import co.cask.cdap.api.metrics.MetricValues;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.BinaryDecoder;
import co.cask.cdap.common.io.DatumReader;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.io.DatumReaderFactory;
import co.cask.cdap.internal.io.SchemaGenerator;
import co.cask.cdap.messaging.MessageFetcher;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.data.RawMessage;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.name.Named;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Process metrics by consuming metrics being published to TMS.
 */
public class MessagingMetricsProcessorService extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(MessagingMetricsProcessorService.class);
  // Log the metrics processing progress no more than once per minute.
  private static final Logger PROGRESS_LOG = Loggers.sampling(LOG, LogSamplers.limitRate(60000));

  private final MetricDatasetFactory metricDatasetFactory;
  private final List<TopicId> metricsTopics;
  private final MessagingService messagingService;
  private final DatumReader<MetricValues> metricReader;
  private final Schema metricSchema;
  private final MetricStore metricStore;
  private final Map<String, String> metricsContextMap;
  private final int fetcherLimit;
  private final long maxDelayMillis;
  private final int queueSize;
  private final BlockingDeque<MetricValues> metricsFromAllTopics;
  private final ConcurrentMap<TopicIdMetaKey, TopicProcessMeta> topicProcessMetaMap;
  private final AtomicBoolean persistingFlag;
  // maximum number of milliseconds to sleep between each run of fetching & processing new metrics
  private final int metricsProcessIntervalMillis;
  private final List<ProcessMetricsThread> processMetricsThreads;
  private final String processMetricName;
  private final int instanceId;
  private final CConfiguration cConfiguration;
  private final boolean skipMigration;
  private final DatasetFramework datasetFramework;
  private final Map<String, String> topicToOldestTimestampDelayMetrics;
  private final Map<String, String> topicToLatestTimestampDelayMetrics;
  private final String metricsPrefixForDelayMetrics;
  private long metricsProcessedCount;

  private MetricsConsumerMetaTable metaTable;
  private ScheduledExecutorService metricsTableDeleterExecutor;
  private DataMigrator metricsDataMigrator;

  private volatile boolean stopping;

  @Inject
  public MessagingMetricsProcessorService(MetricDatasetFactory metricDatasetFactory,
                                          @Named(Constants.Metrics.TOPIC_PREFIX) String topicPrefix,
                                          MessagingService messagingService,
                                          SchemaGenerator schemaGenerator,
                                          DatumReaderFactory readerFactory,
                                          MetricStore metricStore,
                                          @Named(Constants.Metrics.PROCESSOR_MAX_DELAY_MS) long maxDelayMillis,
                                          @Named(Constants.Metrics.QUEUE_SIZE) int queueSize,
                                          @Assisted Set<Integer> topicNumbers,
                                          @Assisted MetricsContext metricsContext,
                                          @Assisted Integer instanceId, DatasetFramework datasetFramework,
                                          CConfiguration cConf) {
    this(metricDatasetFactory, topicPrefix, messagingService, schemaGenerator, readerFactory, metricStore,
         maxDelayMillis, queueSize, topicNumbers, metricsContext, 1000, instanceId,
         datasetFramework, cConf, false);
  }

  @VisibleForTesting
  MessagingMetricsProcessorService(MetricDatasetFactory metricDatasetFactory,
                                   String topicPrefix,
                                   MessagingService messagingService,
                                   SchemaGenerator schemaGenerator,
                                   DatumReaderFactory readerFactory,
                                   MetricStore metricStore,
                                   long maxDelayMillis,
                                   int queueSize,
                                   Set<Integer> topicNumbers,
                                   MetricsContext metricsContext,
                                   int metricsProcessIntervalMillis,
                                   int instanceId, DatasetFramework datasetFramework, CConfiguration cConf,
                                   boolean skipMigration) {
    this.metricDatasetFactory = metricDatasetFactory;
    this.metricsTopics = new ArrayList<>();
    this.topicToLatestTimestampDelayMetrics = new HashMap<>();
    this.topicToOldestTimestampDelayMetrics = new HashMap<>();

    this.metricsPrefixForDelayMetrics = String.format("metrics.processor.%s", instanceId);

    for (int topicNum : topicNumbers) {
      TopicId topicId = NamespaceId.SYSTEM.topic(topicPrefix + topicNum);
      this.metricsTopics.add(topicId);
      topicToOldestTimestampDelayMetrics.put(topicId.getTopic(),
                                             String.format("metrics.processor.%s.topic.%s.oldest.delay.ms",
                                                           instanceId, topicId.getTopic()));
      topicToLatestTimestampDelayMetrics.put(topicId.getTopic(),
                                             String.format("metrics.processor.%s.topic.%s.latest.delay.ms",
                                                           instanceId, topicId.getTopic()));
    }
    this.messagingService = messagingService;
    try {
      this.metricSchema = schemaGenerator.generate(MetricValues.class);
      this.metricReader = readerFactory.create(TypeToken.of(MetricValues.class), metricSchema);
    } catch (UnsupportedTypeException e) {
      // This should never happen
      throw Throwables.propagate(e);
    }
    this.metricStore = metricStore;
    this.metricStore.setMetricsContext(metricsContext);
    this.fetcherLimit = Math.max(1, queueSize / topicNumbers.size()); // fetcherLimit is at least one
    this.maxDelayMillis = maxDelayMillis;
    this.queueSize = queueSize;
    this.metricsContextMap = metricsContext.getTags();
    this.processMetricsThreads = new ArrayList<>();
    this.metricsFromAllTopics = new LinkedBlockingDeque<>(queueSize);
    this.topicProcessMetaMap = new ConcurrentHashMap<>();
    this.persistingFlag = new AtomicBoolean();
    this.metricsProcessIntervalMillis = metricsProcessIntervalMillis;
    this.instanceId = instanceId;
    this.cConfiguration = cConf;
    processMetricName = String.format("metrics.%s.process.count", instanceId);
    this.datasetFramework = datasetFramework;
    // Validate metrics table splits after creation.
    // TODO CDAP-12366 Make metrics table splits configurable
    String metricsTable = cConf.get(Constants.Metrics.METRICS_TABLE_PREFIX,
                                    Constants.Metrics.DEFAULT_METRIC_V3_TABLE_PREFIX + ".ts.1");
    DatasetId metricsTableId = NamespaceId.SYSTEM.dataset(metricsTable);
    try {
      DatasetSpecification spec = datasetFramework.getDatasetSpec(metricsTableId);
      // If v3 metrics table is already exists, we will ignore splits value from cConf to avoid modifying splits
      // after creation
      if (spec != null && cConf.getInt(Constants.Metrics.METRICS_HBASE_TABLE_SPLITS) !=
        spec.getIntProperty(Constants.Metrics.METRICS_HBASE_TABLE_SPLITS, 16)) {
        LOG.warn("Ignoring {} value of property {} from cdap-site.xml because splits value can not be changed for " +
                   "system table {}", cConf.getInt(Constants.Metrics.METRICS_HBASE_TABLE_SPLITS),
                 Constants.Metrics.METRICS_HBASE_TABLE_SPLITS, spec.getName());
      }
    } catch (DatasetManagementException e) {
      LOG.error("Got exception while accessing dataset {}", metricsTable, e);
    }
    this.skipMigration = skipMigration;
  }

  private MetricsConsumerMetaTable getMetaTable() {

    while (metaTable == null) {
      if (stopping) {
        LOG.info("We are shutting down, giving up on acquiring consumer metaTable.");
        break;
      }
      try {
        metaTable = metricDatasetFactory.createConsumerMeta();
      } catch (ServiceUnavailableException e) {
        // No need to log the exception here since this can only happen when the DatasetService is not running.
        // try in next iteration
      } catch (Exception e) {
        LOG.warn("Cannot access consumer metaTable, will retry in 1 sec.");
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    return metaTable;
  }

  @Override
  protected void run() {
    LOG.info("Start running MessagingMetricsProcessorService");
    MetricsConsumerMetaTable metaTable = getMetaTable();
    if (metaTable == null) {
      LOG.info("Could not get MetricsConsumerMetaTable, seems like we are being shut down");
      return;
    }

    for (TopicId topic : metricsTopics) {
      byte[] messageId = null;
      TopicProcessMeta topicProcessMeta = null;
      TopicIdMetaKey topicRowKey = new TopicIdMetaKey(topic);
      try {
        topicProcessMeta = metaTable.getTopicProcessMeta(topicRowKey);
      } catch (Exception e) {
        LOG.warn("Cannot retrieve last processed MessageId for topic: {}", topic, e);
      }
      processMetricsThreads.add(new ProcessMetricsThread(topicRowKey, topicProcessMeta));
    }

    if (!isRunning()) {
      return;
    }

    for (ProcessMetricsThread thread : processMetricsThreads) {
      thread.start();
    }

    if (instanceId == 0) {
      if (!skipMigration) {
        List<Integer> resolutions = new ArrayList<>();
        resolutions.add(Integer.MAX_VALUE);
        resolutions.add(3600);
        resolutions.add(60);

        String v2TableNamePrefix = cConfiguration.get(Constants.Metrics.METRICS_TABLE_PREFIX,
                                                      Constants.Metrics.DEFAULT_METRIC_TABLE_PREFIX) + ".ts.";
        String v3TableNamePrefix = cConfiguration.get(Constants.Metrics.METRICS_TABLE_PREFIX,
                                                      Constants.Metrics.DEFAULT_METRIC_V3_TABLE_PREFIX) + ".ts.";

        int migrationSleepMillis =
          Integer.valueOf(cConfiguration.get(Constants.Metrics.METRICS_MIGRATION_SLEEP_MILLIS));
        metricsDataMigrator = new DataMigrator(datasetFramework, resolutions,
                                               v2TableNamePrefix, v3TableNamePrefix, migrationSleepMillis);
        metricsDataMigrator.run();

        ScheduledExecutorService metricsTableDeleterExecutor =
          Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("metrics-table-deleter"));

        DatasetId v2metrics1sResolutionTable = NamespaceId.SYSTEM.dataset(v2TableNamePrefix + 1);
        MetricsTableDeleter tableDeleter = new MetricsTableDeleter(datasetFramework, v2metrics1sResolutionTable);
        // just schedule deletion of 1 second table to run after 2 hours
        metricsTableDeleterExecutor.schedule(tableDeleter, 2, TimeUnit.HOURS);
      }
    }

    for (ProcessMetricsThread thread : processMetricsThreads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        LOG.info("Thread {} is being terminated while waiting for it to finish.", thread.getName());
        Thread.currentThread().interrupt();
      }
    }

    try {
      // wait upto 5 seconds for the migration to exit cleanly
      if (metricsDataMigrator != null) {
        metricsDataMigrator.join(5000);
      }
    } catch (InterruptedException e) {
      LOG.info("Thread {} is being terminated while waiting for it to finish.", metricsDataMigrator.getName());
      Thread.currentThread().interrupt();
    }

    // Persist metricsFromAllTopics and messageId's after all ProcessMetricsThread's complete.
    // No need to make a copy of metricsFromAllTopics and topicProcessMetaMap because no thread is writing to them
    persistMetricsAndTopicProcessMeta(metricsFromAllTopics, topicProcessMetaMap);
  }

  @Override
  protected void triggerShutdown() {
    LOG.info("Stopping Metrics Processing Service.");
    stopping = true;
    for (ProcessMetricsThread thread : processMetricsThreads) {
      thread.interrupt();
    }
    // set stopping to true for helper class that retries the get/hasInstance/delete dataset
    MigrationTableHelper.requestStop(true);

    if (metricsTableDeleterExecutor != null) {
      metricsTableDeleterExecutor.shutdownNow();
      metricsTableDeleterExecutor = null;
    }
    if (metricsDataMigrator != null) {
      metricsDataMigrator.requestStop();
      metricsDataMigrator.interrupt();
    }
    LOG.info("Metrics Processing Service stopped.");
  }

  /**
   * Persist metrics from all topics into metric store and messageId's of the last persisted metrics of each topic
   * into metrics meta table
   *
   * @param metricValues a deque of {@link MetricValues}
   * @param topicProcessMetaMap a map with each key {@link TopicIdMetaKey} representing a topic
   *                            and {@link TopicProcessMeta} which has info on messageId and processing stats
   */
  private void persistMetricsAndTopicProcessMeta(Deque<MetricValues> metricValues,
                                                 Map<TopicIdMetaKey, TopicProcessMeta> topicProcessMetaMap) {
    try {
      // we update the processing stats in meta table before we persist metrics,
      // in case of huge-delay in writing to metrics table (maybe due to region server issues),
      // this will help surface the last processed timestamp information to the users.
      if (!metricValues.isEmpty()) {
        persistMetrics(metricValues, topicProcessMetaMap);
      }
      persistTopicProcessMeta(topicProcessMetaMap);
    } catch (Exception e) {
      LOG.warn("Failed to persist metrics.", e);
    }
  }

  private void persistTopicProcessMeta(Map<TopicIdMetaKey, TopicProcessMeta> messageIds) {
    try {
      // messageIds can be empty if the current thread fetches nothing while other threads keep fetching new metrics
      // and haven't updated messageId's of the corresponding topics
      if (!messageIds.isEmpty()) {
        metaTable.saveMetricsProcessorStats(messageIds);
      }
    } catch (Exception e) {
      LOG.warn("Failed to update processing stats of consumed messages.", e);
    }
  }

  /**
   * Persist metrics into metric store
   *
   * @param metricValues a non-empty deque of {@link MetricValues}
   */
  private void persistMetrics(Deque<MetricValues> metricValues,
                              Map<TopicIdMetaKey, TopicProcessMeta> topicMetrics) throws Exception {
    long now = System.currentTimeMillis();
    long lastMetricTime = metricValues.peekLast().getTimestamp();
    List<MetricValue> topicLevelDelays = new ArrayList<>();

    //add topic level delay metrics
    for (Map.Entry<TopicIdMetaKey, TopicProcessMeta> entry : topicMetrics.entrySet()) {
      TopicProcessMeta topicProcessMeta = entry.getValue();
      long delay = now - TimeUnit.SECONDS.toMillis(topicProcessMeta.getOldestMetricsTimestamp());
      topicLevelDelays.add(new MetricValue(topicProcessMeta.getOldestMetricsTimestampMetricName(),
                                           MetricType.GAUGE, delay));
      delay = now - TimeUnit.SECONDS.toMillis(topicProcessMeta.getLatestMetricsTimestamp());
      topicLevelDelays.add(new MetricValue(topicProcessMeta.getLatestMetricsTimestampMetricName(),
                                           MetricType.GAUGE, delay));
    }
    List<MetricValue> processorMetrics = new ArrayList<>(topicLevelDelays);
    processorMetrics.add(new MetricValue(processMetricName, MetricType.COUNTER, metricValues.size()));

    metricValues.add(new MetricValues(metricsContextMap, TimeUnit.MILLISECONDS.toSeconds(now), processorMetrics));
    metricStore.add(metricValues);
    metricsProcessedCount += metricValues.size();
    PROGRESS_LOG.debug("{} metrics persisted. Last metric's timestamp: {}",
                       metricsProcessedCount, lastMetricTime);
  }

  private class ProcessMetricsThread extends Thread {
    private final TopicIdMetaKey topicIdMetaKey;
    private final PayloadInputStream payloadInput;
    private final BinaryDecoder decoder;
    private long lastMetricTimeSecs;

    ProcessMetricsThread(TopicIdMetaKey topicIdMetaKey, TopicProcessMeta topicProcessMeta) {
      super(String.format("ProcessMetricsThread-%s", topicIdMetaKey.getTopicId()));
      setDaemon(true);
      String oldestTsMetricName = String.format("%s.topic.%s.oldest.delay.ms",
                                                metricsPrefixForDelayMetrics, topicIdMetaKey.getTopicId().getTopic());
      String latestTsMetricName = String.format("%s.topic.%s.latest.delay.ms",
                                                metricsPrefixForDelayMetrics, topicIdMetaKey.getTopicId().getTopic());
      byte[] persistedMessageId = null;
      if (topicProcessMeta != null && topicProcessMeta.getMessageId() != null) {
        persistedMessageId = topicProcessMeta.getMessageId();
      }
      topicProcessMetaMap.put(topicIdMetaKey,
                              new TopicProcessMeta(persistedMessageId, topicProcessMeta.getOldestMetricsTimestamp(),
                                                   topicProcessMeta.getLatestMetricsTimestamp(),
                                                   topicProcessMeta.getMessagesProcessed(),
                                                   topicProcessMeta.getLastProcessedTimestamp(),
                                                   oldestTsMetricName, latestTsMetricName));
      this.topicIdMetaKey = topicIdMetaKey;
      this.payloadInput = new PayloadInputStream();
      this.decoder = new BinaryDecoder(payloadInput);
    }

    @Override
    public void run() {
      while (isRunning()) {
        try {
          long sleepTime = processMetrics();
          // Don't sleep if sleepTime returned is 0
          if (sleepTime > 0) {
            TimeUnit.MILLISECONDS.sleep(sleepTime);
          }
        } catch (InterruptedException e) {
          // It's triggered by stop
          Thread.currentThread().interrupt();
        }
      }
    }

    /**
     * Fetch at most {@code fetcherLimit} metrics to process, and calculate the estimated sleep time
     * before the next run with the best effort to avoid accumulating unprocessed metrics
     *
     * @return the estimated sleep time before the next run with the best effort to avoid accumulating
     * unprocessed metrics, or {@code 0} if no sleep to catch-up with new metrics at best effort
     */
    private long processMetrics() {
      long startTime = System.currentTimeMillis();
      try {
        MessageFetcher fetcher = messagingService.prepareFetch(topicIdMetaKey.getTopicId());
        fetcher.setLimit(fetcherLimit);
        TopicProcessMeta persistMetaInfo = topicProcessMetaMap.get(topicIdMetaKey);
        byte[] lastMessageId = null;
        if (persistMetaInfo != null) {
          lastMessageId = persistMetaInfo.getMessageId();
        } else {
          // this should not happen as we put topic process meta for the topic id during initialization
          LOG.warn("PersistMetaInfo is null, this should not happen");
        }
        if (lastMessageId != null) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Start fetching from lastMessageId = {}", Bytes.toStringBinary(lastMessageId));
          }
          fetcher.setStartMessage(lastMessageId, false);
        } else {
          LOG.debug("Start fetching from beginning");
          fetcher.setStartTime(0L);
        }

        byte[] currentMessageId = null;
        TopicProcessMeta localTopicProcessMeta =
          new TopicProcessMeta(lastMessageId, Long.MAX_VALUE, Long.MIN_VALUE, 0,
                               TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                               persistMetaInfo.getOldestMetricsTimestampMetricName(),
                               persistMetaInfo.getLatestMetricsTimestampMetricName());
        try (CloseableIterator<RawMessage> iterator = fetcher.fetch()) {
          while (iterator.hasNext() && isRunning()) {
            RawMessage input = iterator.next();
            try {
              payloadInput.reset(input.getPayload());
              MetricValues metricValues = metricReader.read(decoder, metricSchema);
              if (!metricsFromAllTopics.offer(metricValues)) {
                break;
              }
              lastMetricTimeSecs = metricValues.getTimestamp();
              currentMessageId = input.getId();
              if (LOG.isTraceEnabled()) {
                LOG.trace("Received message {} with metrics: {}", Bytes.toStringBinary(currentMessageId), metricValues);
              }

              if (currentMessageId != null) {
                localTopicProcessMeta.updateStats(currentMessageId, lastMetricTimeSecs);
              }
            } catch (IOException e) {
              LOG.warn("Failed to decode message to MetricValue. Skipped. {}", e.getMessage());
            }
          }
        }

        if (currentMessageId != null) {
          localTopicProcessMeta.updateLastProcessedTimestamp();
          topicProcessMetaMap.put(topicIdMetaKey, localTopicProcessMeta);
        }
        // TODO should we try persist only if current messageId is non-null ? currently we seem to try anyways
        // Try to persist metrics and messageId's of the last metrics to be persisted if no other thread is persisting
        tryPersist();

        long endTime = System.currentTimeMillis();
        // use currentMessageId != null to ensure that the current fetching is not empty and
        // lastMetricTimeSecs is updated
        if (currentMessageId != null && endTime - TimeUnit.SECONDS.toMillis(lastMetricTimeSecs) > maxDelayMillis) {
          // Don't sleep if falling behind
          return 0L;
        } else {
          long timeSpent = endTime - startTime;
          return Math.max(0L, metricsProcessIntervalMillis - timeSpent);
        }
      } catch (Exception e) {
        LOG.warn("Failed to process metrics. Will be retried in next iteration.", e);
      }
      return metricsProcessIntervalMillis;
    }

    /**
     * Persist metrics and messageId's of the last metrics to be persisted if no other thread is persisting
     */
    private void tryPersist() {
      // Ensure there's only one thread can persist metricsFromAllTopics and messageId's.
      // If persistingFlag is false, set it to true and start persisting. Otherwise, log and return.
      if (!persistingFlag.compareAndSet(false, true)) {
        LOG.trace("There is another thread performing persisting. No need to persist in this thread.");
        return;
      }
      try {
        // Make a copy of topicProcessMetaMap before copying metrics from metricsFromAllTopics to ensure that
        // topicMessageIdsCopy will not contain new MessageId's in metricsFromAllTopics but not in metricsCopy.
        // This guarantees the metrics corresponding to last persisted MessageId's of each topic are persisted.
        Map<TopicIdMetaKey, TopicProcessMeta> topicProcessMetaMapCopy = new HashMap<>(topicProcessMetaMap);
        // Remove at most queueSize of metrics from metricsFromAllTopics and put into metricsCopy to limit
        // the number of metrics being persisted each time
        Deque<MetricValues> metricsCopy = new LinkedList<>();
        Iterator<MetricValues> iterator = metricsFromAllTopics.iterator();
        while (iterator.hasNext() && metricsCopy.size() < queueSize) {
          metricsCopy.add(iterator.next());
          iterator.remove();
        }
        // Persist the copy of metrics and MessageId's
        persistMetricsAndTopicProcessMeta(metricsCopy, topicProcessMetaMapCopy);
      } catch (Exception e) {
        LOG.warn("Failed to persist metrics. Will be retried in next iteration.", e);
      } finally {
        // Set persistingFlag back to false after persisting completes.
        persistingFlag.set(false);
      }
    }
  }

  private class PayloadInputStream extends ByteArrayInputStream {

    PayloadInputStream() {
      super(Bytes.EMPTY_BYTE_ARRAY);
    }

    void reset(byte[] buf) {
      this.buf = buf;
      this.pos = 0;
      this.count = buf.length;
      this.mark = 0;
    }
  }
}
