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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.BinaryDecoder;
import co.cask.cdap.internal.io.DatumReader;
import co.cask.cdap.internal.io.DatumReaderFactory;
import co.cask.cdap.internal.io.SchemaGenerator;
import co.cask.cdap.messaging.MessageFetcher;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.MessagingUtils;
import co.cask.cdap.messaging.data.RawMessage;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/**
 * Process metrics by consuming metrics being published to TMS.
 */
public class MessagingMetricsProcessorService extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(MessagingMetricsProcessorService.class);

  private final MetricDatasetFactory metricDatasetFactory;
  private final List<TopicId> metricsTopics;
  private final MessagingService messagingService;
  private final DatumReader<MetricValues> recordReader;
  private final Schema recordSchema;
  private final MetricStore metricStore;
  private final int fetcherLimit;
  private final ConcurrentLinkedQueue<MetricValues> records;
  private final ConcurrentMap<TopicIdMetaKey, byte[]> metaKeyMap;
  private final AtomicBoolean persistingFlag;

  private long lastLoggedMillis;
  private long recordsProcessed;

  private Map<String, String> metricsContextMap;
  private List<ProcessMetricsThread> processMetricsThreads;
  private MetricsConsumerMetaTable metaTable;

  private volatile boolean stopping;

  @Inject
  public MessagingMetricsProcessorService(MetricDatasetFactory metricDatasetFactory,
                                          @Named(Constants.Metrics.TOPIC_PREFIX) String topicPrefix,
                                          @Assisted Set<Integer> topicNumbers,
                                          MessagingService messagingService,
                                          SchemaGenerator schemaGenerator,
                                          DatumReaderFactory readerFactory,
                                          MetricStore metricStore,
                                          @Named(Constants.Metrics.MESSAGING_FETCHER_LIMIT) int fetcherLimit) {
    this.metricDatasetFactory = metricDatasetFactory;
    this.metricsTopics = new ArrayList<>();
    for (int topicNum : topicNumbers) {
      this.metricsTopics.add(NamespaceId.SYSTEM.topic(topicPrefix + topicNum));
    }
    this.messagingService = messagingService;
    try {
      this.recordSchema = schemaGenerator.generate(MetricValues.class);
      this.recordReader = readerFactory.create(TypeToken.of(MetricValues.class), recordSchema);
    } catch (UnsupportedTypeException e) {
      // This should never happen
      throw Throwables.propagate(e);
    }
    this.metricStore = metricStore;
    this.fetcherLimit = fetcherLimit;
    this.metricsContextMap = Collections.emptyMap();
    this.processMetricsThreads = new ArrayList<>();
    this.records = new ConcurrentLinkedQueue<>();
    this.metaKeyMap = new ConcurrentHashMap<>();
    this.persistingFlag = new AtomicBoolean();
  }

  public void setMetricsContext(MetricsContext metricsContext) {
    this.metricsContextMap = metricsContext.getTags();
    metricStore.setMetricsContext(metricsContext);
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
    LOG.info("Start running MessagingMetricsProcessorService");
    MetricsConsumerMetaTable metaTable = getMetaTable();
    if (metaTable == null) {
      LOG.info("Could not get MetricsConsumerMetaTable, seems like we are being shut down");
      return;
    }

    for (TopicId topic : metricsTopics) {
      byte[] messageId = null;
      TopicIdMetaKey topicRowKey = new TopicIdMetaKey(topic);
      try {
        messageId = metaTable.getBytes(topicRowKey);
        LOG.info("Last processed MessageId for topic: {} is {}", topic, messageId);
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

  private class ProcessMetricsThread extends Thread {
    private final TopicIdMetaKey topicIdMetaKey;
    private final PayloadInputStream payloadInput;
    private final BinaryDecoder decoder;
    private byte[] lastMessageId;

    ProcessMetricsThread(TopicIdMetaKey topicIdMetaKey, @Nullable byte[] messageId) {
      super(String.format("ProcessMetricsThread-%s", topicIdMetaKey.getTopicId()));
      setDaemon(true);
      this.lastMessageId = messageId;
      this.topicIdMetaKey = topicIdMetaKey;
      this.payloadInput = new PayloadInputStream();
      this.decoder = new BinaryDecoder(payloadInput);
    }

    @Override
    public void run() {
      while (isRunning()) {
        try {
          processMetrics();
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          // It's triggered by stop
          Thread.currentThread().interrupt();
        }
      }
    }

    private void processMetrics() {
      // Decode the metrics records.
      try {
        MessageFetcher fetcher = messagingService.prepareFetch(topicIdMetaKey.getTopicId());
        fetcher.setLimit(fetcherLimit);
        if (lastMessageId != null) {
          LOG.debug("Start fetching from lastMessageId = {}", lastMessageId);
          fetcher.setStartMessage(lastMessageId, false);
        } else {
          LOG.debug("Start fetching from beginning");
          fetcher.setStartTime(0L);
        }

        byte[] currentMessageId = null;
        String lastMessageName = null;
        try (CloseableIterator<RawMessage> iterator = fetcher.fetch()) {
          while (iterator.hasNext() && isRunning()) {
            RawMessage input = iterator.next();
            try {
              payloadInput.reset(input.getPayload());
              MetricValues metricValues = recordReader.read(decoder, recordSchema);
              records.add(metricValues);
              currentMessageId = input.getId();
              lastMessageName = Iterables.getOnlyElement(metricValues.getMetrics()).getName();
              LOG.debug("currentMessageId = {}, currentMessageName = {}, time = {}", currentMessageId, lastMessageName,
                        metricValues.getTimestamp());
              LOG.trace("Received message {} with metrics: {}", Bytes.toStringBinary(lastMessageId), metricValues);
            } catch (IOException e) {
              LOG.warn("Failed to decode message to MetricValue. Skipped. {}", e.getMessage());
            }
          }
        }

        if (currentMessageId != null) {
          metaKeyMap.put(topicIdMetaKey, currentMessageId);
          lastMessageId = currentMessageId;
          LOG.debug("lastMessageId = {}, lastMessageName = {}", lastMessageId, lastMessageName);
        }

        if (records.isEmpty()) {
          LOG.trace("No metrics record to process.");
          return;
        }

        // Ensure there's only one thread can persist records when the persistingFlag is false.
        // The thread set persistingFlag to true when it starts to persist.
        if (!persistingFlag.compareAndSet(false, true)) {
          LOG.debug("Cannot persist because persistingFlag is grabbed.");
          return;
        }
        List<MetricValues> recordsCopy = new ArrayList<>(records.size());
        while (!records.isEmpty()) {
          recordsCopy.add(records.poll());
        }
        Map<TopicIdMetaKey, byte[]> metaKeyMapCopy = new HashMap<>(metaKeyMap);
        LOG.debug("records after removal: {}", records);
        LOG.debug("Persisting recordsCopy: {}", recordsCopy);
        List<String> metaKeys = new ArrayList<>();
        for (Map.Entry<TopicIdMetaKey, byte[]> entry : metaKeyMapCopy.entrySet()) {
          metaKeys.add(entry.getKey().getTopicId().toString() + ":" + entry.getValue());
        }
        LOG.debug("Persisting recordsCopy: {}", metaKeys);
        try {
          persistRecords(recordsCopy);
          persistMessageIds(metaKeyMapCopy);
        } catch (Exception e) {
          // Simple log and ignore the error.
          LOG.error("Failed to persist consumed messages.", e);
        } finally {
          // Set persistingFlag back to false after persisting completes.
          persistingFlag.set(false);
        }
      } catch (Exception e) {
        LOG.error("Failed to process metrics. Will be retried in next iteration.", e);
      }
    }

    private void persistRecords(List<MetricValues> metricValues) throws Exception {
      long now = System.currentTimeMillis();
      long lastRecordTime = metricValues.get(metricValues.size() - 1).getTimestamp();
      long delay = now - TimeUnit.SECONDS.toMillis(lastRecordTime);
      metricValues.add(
        new MetricValues(metricsContextMap, TimeUnit.MILLISECONDS.toSeconds(now), ImmutableList.of(
          new MetricValue("metrics.process.count", MetricType.COUNTER, metricValues.size()),
          new MetricValue("metrics.process.delay.ms", MetricType.GAUGE, delay))));
      metricStore.add(metricValues);
      recordsProcessed += metricValues.size();
      // avoid logging more than once a minute
      if (now > lastLoggedMillis + TimeUnit.MINUTES.toMillis(1)) {
        lastLoggedMillis = now;
        LOG.debug("{} metrics records processed in thread {}. Last metric record's timestamp: {}. " +
                    "Metrics process delay: {}",
                  recordsProcessed, getName(), lastRecordTime, delay);
      }
    }

    private void persistMessageIds(Map<TopicIdMetaKey, byte[]> metaKeyMap) {
      try {
        metaTable.saveMessageIds(metaKeyMap);
      } catch (Exception e) {
        LOG.error("Failed to persist consumed messages messageId's {}", metaKeyMap, e);
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
