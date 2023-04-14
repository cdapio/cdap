/*
 * Copyright © 2017-2023 Cask Data, Inc.
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

package io.cdap.cdap.metrics.process;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.metrics.MetricType;
import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.metrics.MetricsWriter;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.BinaryDecoder;
import io.cdap.cdap.common.io.DatumReader;
import io.cdap.cdap.common.io.StringCachingDecoder;
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import io.cdap.cdap.common.utils.ResettableByteArrayInputStream;
import io.cdap.cdap.internal.io.DatumReaderFactory;
import io.cdap.cdap.internal.io.SchemaGenerator;
import io.cdap.cdap.messaging.MessageFetcher;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.data.RawMessage;
import io.cdap.cdap.metrics.store.MetricDatasetFactory;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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
  private final MetricsWriter metricsWriter;
  private final Map<String, String> metricsContextMap;
  private final int fetcherLimit;
  private final long maxDelayMillis;
  private final int queueSize;
  private final long offerTimeoutMillis;
  private final BlockingDeque<MetricValues> metricsFromAllTopics;
  private final AtomicBoolean persistingFlag;
  private final boolean limitWriteFrequency;
  private final MetadataHandler metadataHandler;
  private final MetricsMetaKeyProvider metricsMetaKeyProvider;
  // maximum number of milliseconds to sleep between each run of fetching & processing new metrics, the max sleep time
  // is 1 min
  private final long metricsProcessIntervalMillis;
  private final List<ProcessMetricsThread> processMetricsThreads;
  private final String processMetricName;
  private final String metricsPrefixForDelayMetrics;
  private final int instanceId;
  private long metricsProcessedCount;
  private AtomicLong lastPersistedTime;
  private MetricsConsumerMetaTable metaTable;
  private volatile boolean stopping;

  @Inject
  MessagingMetricsProcessorService(CConfiguration cConf,
      MetricDatasetFactory metricDatasetFactory,
      MessagingService messagingService,
      SchemaGenerator schemaGenerator,
      DatumReaderFactory readerFactory,
      MetricsWriter metricsWriter,
      MetadataHandler metadataHandler,
      MetricsMetaKeyProvider metricsMetaKeyProvider,
      @Assisted Set<Integer> topicNumbers,
      @Assisted MetricsContext metricsContext,
      @Assisted Integer instanceId) {
    this(cConf, metricDatasetFactory, messagingService,
        schemaGenerator, readerFactory, metricsWriter, topicNumbers, metricsContext,
        TimeUnit.SECONDS.toMillis(
            cConf.getInt(Constants.Metrics.METRICS_MINIMUM_RESOLUTION_SECONDS)), instanceId,
        metadataHandler, metricsMetaKeyProvider);
  }

  @VisibleForTesting
  MessagingMetricsProcessorService(CConfiguration cConf,
      MetricDatasetFactory metricDatasetFactory,
      MessagingService messagingService,
      SchemaGenerator schemaGenerator,
      DatumReaderFactory readerFactory,
      MetricsWriter metricsWriter,
      Set<Integer> topicNumbers,
      MetricsContext metricsContext,
      long metricsProcessIntervalMillis,
      int instanceId,
      MetadataHandler metadataHandler,
      MetricsMetaKeyProvider metricsMetaKeyProvider) {
    this.metricDatasetFactory = metricDatasetFactory;
    this.metricsPrefixForDelayMetrics = String.format("metrics.processor.%s.%s", instanceId,
        metricsWriter.getID());

    String topicPrefix = cConf.get(Constants.Metrics.TOPIC_PREFIX);
    this.metricsTopics = topicNumbers.stream()
        .map(n -> NamespaceId.SYSTEM.topic(topicPrefix + n))
        .collect(Collectors.toList());
    this.messagingService = messagingService;
    try {
      this.metricSchema = schemaGenerator.generate(MetricValues.class);
      this.metricReader = readerFactory.create(TypeToken.of(MetricValues.class), metricSchema);
    } catch (UnsupportedTypeException e) {
      // This should never happen
      throw Throwables.propagate(e);
    }
    this.metricsWriter = metricsWriter;
    this.maxDelayMillis = cConf.getLong(Constants.Metrics.PROCESSOR_MAX_DELAY_MS);
    this.queueSize = cConf.getInt(Constants.Metrics.QUEUE_SIZE);
    this.offerTimeoutMillis = cConf.getInt(Constants.Metrics.OFFER_TIMEOUT_MS);
    this.fetcherLimit = Math.max(1,
        queueSize / topicNumbers.size()); // fetcherLimit is at least one
    this.metricsContextMap = metricsContext.getTags();
    this.processMetricsThreads = new ArrayList<>();
    this.metricsFromAllTopics = new LinkedBlockingDeque<>(queueSize);
    this.persistingFlag = new AtomicBoolean();
    // the max sleep time will be 1 min
    this.metricsProcessIntervalMillis = resolveProcessingInterval(cConf, metricsWriter,
        metricsProcessIntervalMillis);
    this.processMetricName = String.format("metrics.%s.process.count", instanceId);
    this.metadataHandler = metadataHandler;
    this.metricsMetaKeyProvider = metricsMetaKeyProvider;
    this.instanceId = instanceId;
    this.limitWriteFrequency = shouldLimitWriteFrequency(metricsWriter, cConf);
  }

  private MetricsConsumerMetaTable getMetaTable() {
    while (metaTable == null) {
      if (stopping) {
        LOG.info("We are shutting down, giving up on acquiring consumer metaTable.");
        break;
      }
      try {
        metaTable = metricDatasetFactory.createConsumerMeta();
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
    LOG.info("Start running MessagingMetricsProcessorService for {}", metricsWriter.getID());

    MetricsConsumerMetaTable metaTable = getMetaTable();
    if (metaTable == null) {
      LOG.info("Could not get MetricsConsumerMetaTable, seems like we are being shut down");
      return;
    }

    metadataHandler.initCache(metricsTopics, metaTable);
    Map<TopicId, MetricsMetaKey> keys = metricsMetaKeyProvider.getKeys(metricsTopics);
    for (Map.Entry<TopicId, MetricsMetaKey> keyEntry : keys.entrySet()) {
      ProcessMetricsThread metricsThread = new ProcessMetricsThread(keyEntry.getKey(),
          keyEntry.getValue(),
          String.format("processor-%s-%s", instanceId,
              metricsWriter.getID()));
      processMetricsThreads.add(metricsThread);
    }

    if (!isRunning()) {
      return;
    }

    // Initialize the last persisted time with current time.
    // This will give time for the threads to populate some metrics before persisting
    lastPersistedTime = new AtomicLong(System.currentTimeMillis());
    for (ProcessMetricsThread thread : processMetricsThreads) {
      thread.start();
    }

    for (ProcessMetricsThread thread : processMetricsThreads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        LOG.info("Thread {} is being terminated while waiting for it to finish.", thread.getName());
        Thread.currentThread().interrupt();
      }
    }

    // Persist metricsFromAllTopics and messageId's after all ProcessMetricsThread's complete.
    // No need to make a copy of metricsFromAllTopics and topicProcessMetaMap because no thread is writing to them
    persistMetricsAndTopicProcessMeta(metricsFromAllTopics, metadataHandler.getCache());
  }

  private long resolveProcessingInterval(CConfiguration cConf, MetricsWriter metricsWriter,
      long defaultInterval) {
    String writeFreqConfig = String.format(Constants.Metrics.WRITER_WRITE_FREQUENCY_SECONDS,
        metricsWriter.getID());
    int writeFreq = cConf.getInt(writeFreqConfig, -1);
    return writeFreq == -1
        ? Math.min(defaultInterval, Constants.Metrics.PROCESS_INTERVAL_MILLIS)
        : TimeUnit.SECONDS.toMillis(writeFreq);
  }

  private boolean shouldLimitWriteFrequency(MetricsWriter writer, CConfiguration cConf) {
    // If there is no writer specific configuration, default to existing behavior of no limit
    String confKey = String.format(Constants.Metrics.WRITER_LIMIT_WRITE_FREQ, writer.getID());
    return cConf.getBoolean(confKey, false);
  }

  @Override
  protected void triggerShutdown() {
    LOG.info("Stopping Metrics Processing Service.");
    stopping = true;
    for (ProcessMetricsThread thread : processMetricsThreads) {
      thread.interrupt();
    }
    LOG.info("Metrics Processing Service stopped.");
  }

  /**
   * Persist metrics from all topics into metric store and messageId's of the last persisted metrics
   * of each topic into metrics meta table
   *
   * @param metricValues a deque of {@link MetricValues}
   * @param topicProcessMetaMap a map with each key {@link MetricsMetaKey} representing a key
   *     and {@link TopicProcessMeta} which has info on messageId and processing stats
   */
  private void persistMetricsAndTopicProcessMeta(Deque<MetricValues> metricValues,
      Map<MetricsMetaKey, TopicProcessMeta> topicProcessMetaMap) {
    try {
      if (!metricValues.isEmpty()) {
        persistMetrics(metricValues);
      }
      metadataHandler.saveCache(topicProcessMetaMap);
    } catch (Exception e) {
      LOG.warn("Failed to persist metrics.", e);
    }
  }


  /**
   * Persist metrics into metric store
   *
   * @param metricValues a non-empty deque of {@link MetricValues}
   */
  private void persistMetrics(Deque<MetricValues> metricValues) {
    long now = System.currentTimeMillis();
    long lastMetricTime = metricValues.peekLast().getTimestamp();
    List<MetricValue> topicLevelDelays = new ArrayList<>();

    //write topic level delay metrics
    for (TopicProcessMeta topicProcessMeta : metadataHandler.getCache().values()) {
      long delay = now - TimeUnit.SECONDS.toMillis(topicProcessMeta.getOldestMetricsTimestamp());
      topicLevelDelays.add(new MetricValue(topicProcessMeta.getOldestMetricsTimestampMetricName(),
          MetricType.GAUGE, delay));
      delay = now - TimeUnit.SECONDS.toMillis(topicProcessMeta.getLatestMetricsTimestamp());
      topicLevelDelays.add(new MetricValue(topicProcessMeta.getLatestMetricsTimestampMetricName(),
          MetricType.GAUGE, delay));
    }
    List<MetricValue> processorMetrics = new ArrayList<>(topicLevelDelays);
    processorMetrics.add(
        new MetricValue(processMetricName, MetricType.COUNTER, metricValues.size()));

    long nowSeconds = TimeUnit.MILLISECONDS.toSeconds(now);
    metricValues.add(new MetricValues(metricsContextMap, nowSeconds, processorMetrics));
    metricsWriter.write(metricValues);
    metricsProcessedCount += metricValues.size();
    PROGRESS_LOG.debug(
        "{}(+{}) metrics persisted with {}. Last metric's timestamp: {} (lag {} seconds)",
        metricsProcessedCount, metricValues.size(), metricsWriter.getID(),
        lastMetricTime, nowSeconds - lastMetricTime);
  }

  private class ProcessMetricsThread extends Thread {

    private final MetricsMetaKey metricsMetaKey;
    private final TopicId topic;
    private final ResettableByteArrayInputStream payloadInput;
    private final StringCachingDecoder decoder;
    private final String oldestTsMetricName;
    private final String latestTsMetricName;
    private long lastMetricTimeSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

    ProcessMetricsThread(TopicId topic, MetricsMetaKey metricsMetaKey, String processorName) {
      //TODO - create a unique thread name
      super(String.format("ProcessMetricsThread-%s-%s", topic, processorName));
      setDaemon(true);
      oldestTsMetricName = String.format("%s.topic.%s.oldest.delay.ms",
          metricsPrefixForDelayMetrics, topic.getTopic());
      latestTsMetricName = String.format("%s.topic.%s.latest.delay.ms",
          metricsPrefixForDelayMetrics, topic.getTopic());
      this.metricsMetaKey = metricsMetaKey;
      this.topic = topic;
      this.payloadInput = new ResettableByteArrayInputStream();
      this.decoder = new StringCachingDecoder(new BinaryDecoder(payloadInput), new HashMap<>());
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
     * @return the estimated sleep time before the next run with the best effort to avoid
     *     accumulating unprocessed metrics, or {@code 0} if no sleep to catch-up with new metrics
     *     at best effort
     */
    private long processMetrics() {
      long startTime = System.currentTimeMillis();
      try {
        // Before retrieving try to free up some queue space by persisting metrics and messageId's
        // if no other thread is persisting
        tryPersist();

        MessageFetcher fetcher = messagingService.prepareFetch(topic);
        fetcher.setLimit(fetcherLimit);
        TopicProcessMeta persistMetaInfo = metadataHandler.getTopicProcessMeta(metricsMetaKey);
        byte[] lastMessageId = null;

        if (persistMetaInfo != null) {
          lastMessageId = persistMetaInfo.getMessageId();
        }

        if (lastMessageId != null) {
          fetcher.setStartMessage(lastMessageId, false);
        } else {
          fetcher.setStartTime(0L);
        }

        byte[] currentMessageId = null;
        boolean gotMessages;
        TopicProcessMeta localTopicProcessMeta =
            new TopicProcessMeta(lastMessageId, Long.MAX_VALUE, Long.MIN_VALUE, 0,
                TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                oldestTsMetricName, latestTsMetricName);
        try (CloseableIterator<RawMessage> iterator = fetcher.fetch()) {
          gotMessages = iterator.hasNext();
          while (iterator.hasNext() && isRunning()) {
            RawMessage input = iterator.next();
            try {
              payloadInput.reset(input.getPayload());
              MetricValues metricValues = metricReader.read(decoder, metricSchema);
              if (currentMessageId == null) {
                //For the first message we are willing to wait for space in queue
                if (!metricsFromAllTopics.offer(metricValues, offerTimeoutMillis,
                    TimeUnit.MILLISECONDS)) {
                  break;
                }
              } else if (!metricsFromAllTopics.offer(metricValues)) {
                break;
              }
              lastMetricTimeSecs = metricValues.getTimestamp();
              currentMessageId = input.getId();
              if (LOG.isTraceEnabled()) {
                LOG.trace("Received message {} with metrics: {}",
                    Bytes.toStringBinary(currentMessageId), metricValues);
              }
              localTopicProcessMeta.updateTopicProcessingStats(currentMessageId,
                  lastMetricTimeSecs);
            } catch (IOException e) {
              LOG.warn("Failed to decode message to MetricValue. Skipped. {}", e.getMessage());
            }
          }
        }
        decoder.getCache().clear();

        if (currentMessageId != null) {
          // update the last processed timestamp in local topic meta and update the topicProcessMetaMap with this
          // local topic meta for the topic
          localTopicProcessMeta.updateLastProcessedTimestamp();
          metadataHandler.updateCache(metricsMetaKey, localTopicProcessMeta);
        }
        // Try to persist metrics and messageId's of the last metrics to be persisted if no other thread is persisting
        tryPersist();

        long endTime = System.currentTimeMillis();
        if (gotMessages
            && endTime - TimeUnit.SECONDS.toMillis(lastMetricTimeSecs) > maxDelayMillis) {
          // Don't sleep if falling behind
          return 0L;
        } else {
          long timeSpent = endTime - startTime;
          return Math.max(0L, metricsProcessIntervalMillis - timeSpent);
        }
      } catch (InterruptedException e) {
        LOG.trace("Thread interrupted while processing metrics. Probably stopping now.", e);
        return 0L;
      } catch (ServiceUnavailableException e) {
        LOG.trace("Could not fetch metrics. Will be retried in next iteration.", e);
      } catch (Exception e) {
        LOG.warn("Failed to process metrics. Will be retried in next iteration.", e);
      }
      return metricsProcessIntervalMillis;
    }

    /**
     * Persist metrics and messageId's of the last metrics to be persisted if no other thread is
     * persisting
     */
    private void tryPersist() {
      if (!canPersist()) {
        return;
      }

      try {
        // Make a copy of topicProcessMetaMap before copying metrics from metricsFromAllTopics to ensure that
        // topicMessageIdsCopy will not contain new MessageId's in metricsFromAllTopics but not in metricsCopy.
        // This guarantees the metrics corresponding to last persisted MessageId's of each topic are persisted.
        Map<MetricsMetaKey, TopicProcessMeta> topicProcessMetaMapCopy = metadataHandler.getCache();
        // Remove at most queueSize of metrics from metricsFromAllTopics and put into metricsCopy to limit
        // the number of metrics being persisted each time
        Deque<MetricValues> metricsCopy = new LinkedList<>();
        Iterator<MetricValues> iterator = metricsFromAllTopics.iterator();
        // Though the blocking queue(metricsFromAllTopics) has upper bound on its size (which is the "queueSize")
        // there can be a scenario, as the current thread is removing entries from blocking queue
        // and adding it to a copy list, other threads are simultaneously adding entries to the queue and
        // the current list might become very big causing out of memory issues, we avoid this
        // by making the copy list size also to be limited by the max queue size.
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

  private boolean canPersist() {
    // Check if writer has restriction on write frequency
    if (writeFrequencyExceeded()) {
      return false;
    }
    // Ensure there's only one thread can persist metricsFromAllTopics and messageId's.
    // This is because the underlying metrics table is not thread safe.
    // If persistingFlag is false, set it to true and start persisting. Otherwise, log and return.
    if (!persistingFlag.compareAndSet(false, true)) {
      LOG.trace(
          "There is another thread performing persisting. No need to persist in this thread.");
      return false;
    }

    return true;
  }

  private boolean writeFrequencyExceeded() {
    if (!limitWriteFrequency) {
      return false;
    }
    long currentTime = System.currentTimeMillis();
    long updatedTime = lastPersistedTime.updateAndGet(
        value -> (currentTime - value > metricsProcessIntervalMillis) ? currentTime : value);
    if (updatedTime != currentTime) {
      LOG.trace("Not enough time between writes.");
      return true;
    }
    return false;
  }
}
