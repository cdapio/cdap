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
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.internal.io.DatumReaderFactory;
import co.cask.cdap.internal.io.SchemaGenerator;
import co.cask.cdap.messaging.MessageFetcher;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.MessagingUtils;
import co.cask.cdap.messaging.data.RawMessage;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import javax.annotation.Nullable;

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
  private final ConcurrentMap<TopicIdMetaKey, byte[]> topicMessageIds;
  private final AtomicBoolean persistingFlag;
  // maximum number of milliseconds to sleep between each run of fetching & processing new metrics
  private final int metricsProcessIntervalMillis;
  private final List<ProcessMetricsThread> processMetricsThreads;
  private final String processMetricName;
  private final String delayMetricName;
  private final int instanceId;
  private final CConfiguration cConfiguration;
  private final Configuration hConf;

  private long metricsProcessedCount;

  private MetricsConsumerMetaTable metaTable;

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
                                          @Assisted Integer instanceId,
                                          CConfiguration cConfiguration,
                                          Configuration hConf) {
    this(metricDatasetFactory, topicPrefix, messagingService, schemaGenerator, readerFactory, metricStore,
         maxDelayMillis, queueSize, topicNumbers, metricsContext, 1000, instanceId, cConfiguration, hConf);
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
                                   int instanceId, CConfiguration cConfiguration, Configuration hConf) {
    this.metricDatasetFactory = metricDatasetFactory;
    this.metricsTopics = new ArrayList<>();
    for (int topicNum : topicNumbers) {
      this.metricsTopics.add(NamespaceId.SYSTEM.topic(topicPrefix + topicNum));
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
    this.topicMessageIds = new ConcurrentHashMap<>();
    this.persistingFlag = new AtomicBoolean();
    this.metricsProcessIntervalMillis = metricsProcessIntervalMillis;
    this.instanceId = instanceId;
    this.cConfiguration = cConfiguration;
    this.hConf = hConf;
    processMetricName = String.format("metrics.%s.process.count", instanceId);
    delayMetricName = String.format("metrics.%s.process.delay.ms", instanceId);
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

    if (instanceId == 0) {
      // todo iterate migration resolution order list and schedule execution based on data availability.

      ScheduledExecutorService scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("metrics-data-migrator"));

      DataMigrator dataMigrator = new DataMigrator(metricDatasetFactory, cConfiguration, hConf);
      // todo change interval with config.
      scheduledExecutorService.scheduleAtFixedRate(dataMigrator, 0, 1, TimeUnit.MINUTES);
      LOG.info("Scheduled metrics migration thread");
    }

    for (TopicId topic : metricsTopics) {
      byte[] messageId = null;
      TopicIdMetaKey topicRowKey = new TopicIdMetaKey(topic);
      try {
        messageId = metaTable.getBytes(topicRowKey);
      } catch (Exception e) {
        LOG.warn("Cannot retrieve last processed MessageId for topic: {}", topic, e);
      }
      processMetricsThreads.add(new ProcessMetricsThread(topicRowKey, messageId));
    }

    if (!isRunning()) {
      return;
    }

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
    // No need to make a copy of metricsFromAllTopics and topicMessageIds because no thread is writing to them
    persistMetricsMessageIds(metricsFromAllTopics, topicMessageIds);
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
   * Persist metrics from all topics into metric store and messageId's of the last persisted metrics of each topic
   * into metrics meta table
   *
   * @param metricValues a deque of {@link MetricValues}
   * @param messageIds a map with each key {@link TopicIdMetaKey} representing a topic and messageId's
   *                   of the last persisted metric of the topic
   */
  private void persistMetricsMessageIds(Deque<MetricValues> metricValues, Map<TopicIdMetaKey, byte[]> messageIds) {
    try {
      if (!metricValues.isEmpty()) {
        persistMetrics(metricValues);
      }
      persistMessageIds(messageIds);
    } catch (Exception e) {
      LOG.warn("Failed to persist metrics.", e);
    }
  }

  /**
   * Persist metrics into metric store
   *
   * @param metricValues a non-empty deque of {@link MetricValues}
   */
  private void persistMetrics(Deque<MetricValues> metricValues) throws Exception {
    long now = System.currentTimeMillis();
    long lastMetricTime = metricValues.peekLast().getTimestamp();
    long delay = now - TimeUnit.SECONDS.toMillis(lastMetricTime);
    metricValues.add(
      new MetricValues(metricsContextMap, TimeUnit.MILLISECONDS.toSeconds(now),
                       ImmutableList.of(
                         new MetricValue(processMetricName, MetricType.COUNTER, metricValues.size()),
                         new MetricValue(delayMetricName, MetricType.GAUGE, delay))));
    metricStore.add(metricValues);
    metricsProcessedCount += metricValues.size();
    PROGRESS_LOG.debug("{} metrics metrics persisted. Last metric metric's timestamp: {}. " +
                         "Metrics process delay: {}ms", metricsProcessedCount, lastMetricTime, delay);
  }

  /**
   * Persist messageId's of the last persisted metrics of each topic into metrics meta table
   *
   * @param messageIds   a map with each key {@link TopicIdMetaKey} representing a topic and messageId's
   *                     of the last persisted metric of the topic
   */
  private void persistMessageIds(Map<TopicIdMetaKey, byte[]> messageIds) {
    try {
      // messageIds can be empty if the current thread fetches nothing while other threads keep fetching new metrics
      // and haven't updated messageId's of the corresponding topics
      if (!messageIds.isEmpty()) {
        metaTable.saveMessageIds(messageIds);
      }
    } catch (Exception e) {
      LOG.warn("Failed to persist messageId's of consumed messages.", e);
    }
  }

  private class ProcessMetricsThread extends Thread {
    private final TopicIdMetaKey topicIdMetaKey;
    private final PayloadInputStream payloadInput;
    private final BinaryDecoder decoder;
    private long lastMetricTimeSecs;

    ProcessMetricsThread(TopicIdMetaKey topicIdMetaKey, @Nullable byte[] messageId) {
      super(String.format("ProcessMetricsThread-%s", topicIdMetaKey.getTopicId()));
      setDaemon(true);
      if (messageId != null) {
        topicMessageIds.put(topicIdMetaKey, messageId);
      }
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
        byte[] lastMessageId = topicMessageIds.get(topicIdMetaKey);
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
            } catch (IOException e) {
              LOG.warn("Failed to decode message to MetricValue. Skipped. {}", e.getMessage());
            }
          }
        }

        if (currentMessageId != null) {
          topicMessageIds.put(topicIdMetaKey, currentMessageId);
        }

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
        // Make a copy of topicMessageIds before copying metrics from metricsFromAllTopics to ensure that
        // topicMessageIdsCopy will not contain new MessageId's in metricsFromAllTopics but not in metricsCopy.
        // This guarantees the metrics corresponding to last persisted MessageId's of each topic are persisted.
        Map<TopicIdMetaKey, byte[]> topicMessageIdsCopy = new HashMap<>(topicMessageIds);
        // Remove at most queueSize of metrics from metricsFromAllTopics and put into metricsCopy to limit
        // the number of metrics being persisted each time
        Deque<MetricValues> metricsCopy = new LinkedList<>();
        Iterator<MetricValues> iterator = metricsFromAllTopics.iterator();
        while (iterator.hasNext() && metricsCopy.size() < queueSize) {
          metricsCopy.add(iterator.next());
          iterator.remove();
        }
        // Persist the copy of metrics and MessageId's
        persistMetricsMessageIds(metricsCopy, topicMessageIdsCopy);
      } catch (Exception e) {
        LOG.warn("Failed to persist metrics. Will be retried in next iteration.", e);
      } finally {
        // Set persistingFlag back to false after persisting completes.
        persistingFlag.set(false);
      }
    }
  }

  private final class TopicIdMetaKey implements MetricsMetaKey {

    private final TopicId topicId;
    private final byte[] key;

    TopicIdMetaKey(TopicId topicId) {
      this.topicId = topicId;
      this.key = MessagingUtils.toMetadataRowKey(topicId);
    }

    @Override
    public byte[] getKey() {
      return key;
    }

    TopicId getTopicId() {
      return topicId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TopicIdMetaKey that = (TopicIdMetaKey) o;
      // Comparing the key is enough because key and topicId have one-to-one relationship
      return Arrays.equals(getKey(), that.getKey());
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(getKey());
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
