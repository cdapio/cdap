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
import com.google.common.collect.Lists;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
    private final List<MetricValues> records;
    private final PayloadInputStream payloadInput;
    private final BinaryDecoder decoder;
    private byte[] lastMessageId;

    ProcessMetricsThread(TopicIdMetaKey topicIdMetaKey, @Nullable byte[] messageId) {
      super(String.format("ProcessMetricsThread-%s", topicIdMetaKey.getTopicId()));
      setDaemon(true);
      this.lastMessageId = messageId;
      this.topicIdMetaKey = topicIdMetaKey;
      this.records = Lists.newArrayList();
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
          fetcher.setStartMessage(lastMessageId, false);
        } else {
          fetcher.setStartTime(0L);
        }

        byte[] nextMessageId = null;
        try (CloseableIterator<RawMessage> iterator = fetcher.fetch()) {
          while (iterator.hasNext() && isRunning()) {
            RawMessage input = iterator.next();
            try {
              payloadInput.reset(input.getPayload());
              MetricValues metricValues = recordReader.read(decoder, recordSchema);
              records.add(metricValues);
              nextMessageId = input.getId();
              LOG.trace("Received message {} with metrics: {}", Bytes.toStringBinary(lastMessageId), metricValues);
            } catch (IOException e) {
              LOG.warn("Failed to decode message to MetricValue. Skipped. {}", e.getMessage());
            }
          }
        }

        if (records.isEmpty()) {
          LOG.trace("No metrics record to process.");
          return;
        }

        persistRecords(records);
        records.clear();
        lastMessageId = nextMessageId;
        persistMessageId(lastMessageId);
      } catch (Exception e) {
        LOG.error("Failed to process metrics. Will be retried in next iteration.", e);
      }
    }

    private void persistRecords(List<MetricValues> metricValues) throws Exception {
      long now = System.currentTimeMillis();
      addProcessingStats(metricValues, now);
      metricStore.add(metricValues);
      recordsProcessed += metricValues.size();
      // avoid logging more than once a minute
      if (now > lastLoggedMillis + TimeUnit.MINUTES.toMillis(1)) {
        lastLoggedMillis = now;
        LOG.debug("{} metrics records processed in thread {}. Last record time: {}.",
                 recordsProcessed, getName(), metricValues.get(metricValues.size() - 1).getTimestamp());
      }
    }

    private void addProcessingStats(List<MetricValues> records, long currentTime) {
      int count = records.size();
      long delay = currentTime - TimeUnit.SECONDS.toMillis(records.get(records.size() - 1).getTimestamp());
      records.add(
        new MetricValues(metricsContextMap, TimeUnit.MILLISECONDS.toSeconds(currentTime),
                         ImmutableList.of(new MetricValue("metrics.process.count", MetricType.COUNTER, count),
                                          new MetricValue("metrics.process.delay.ms", MetricType.GAUGE, delay))));
    }

    private void persistMessageId(byte[] messageId) {
      try {
        metaTable.save(topicIdMetaKey, messageId);
        LOG.debug("Persisted last processed MessageId: {} in thread {}", Bytes.toStringBinary(messageId), getName());
      } catch (Exception e) {
        // Simple log and ignore the error.
        LOG.error("Failed to persist consumed messageId {}", Bytes.toStringBinary(messageId), e);
      }
    }
  }

  private class TopicIdMetaKey implements MetricsMetaKey {

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
