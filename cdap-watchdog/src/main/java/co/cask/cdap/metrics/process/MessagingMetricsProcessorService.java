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
import java.util.Arrays;
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

  private static final byte[] INITIAL_LAST_MESSAGE_ID = Bytes.toBytes(-1L);

  @Nullable
  private MetricsContext metricsContext;

  private final MetricDatasetFactory metricDatasetFactory;
  private final TopicId[] metricsTopics;
  private final TopicIdMetaKey[] metricsTopicMetaKeys;
  private final MessagingService messagingService;
  private final DatumReader<MetricValues> recordReader;
  private final Schema recordSchema;
  private final MetricStore metricStore;
  private final int fetcherLimit;

  private long lastLoggedMillis;
  private long recordsProcessed;

  private Map<String, String> metricsContextMap;
  private List<ProcessMetricsThread> processMetricsThreadList;
  private MetricsConsumerMetaTable metaTable;

  private volatile boolean stopping = false;

  @Inject
  public MessagingMetricsProcessorService(MetricDatasetFactory metricDatasetFactory,
                                          @Named(Constants.Metrics.TOPIC_PREFIX) String topicPrefix,
                                          @Assisted Set<Integer> topicNumbers,
                                          MessagingService messagingService,
                                          SchemaGenerator schemaGenerator,
                                          DatumReaderFactory readerFactory,
                                          MetricStore metricStore,
                                          @Named(Constants.Metrics.MESSAGING_FETCHER_LIMIT)
                                            int fetcherLimit) {
    this.metricDatasetFactory = metricDatasetFactory;
    this.metricsTopics = new TopicId[topicNumbers.size()];
    this.metricsTopicMetaKeys = new TopicIdMetaKey[topicNumbers.size()];
    int i = 0;
    for (int topicNum : topicNumbers) {
      this.metricsTopics[i] = NamespaceId.SYSTEM.topic(topicPrefix + topicNum);
      this.metricsTopicMetaKeys[i] = new TopicIdMetaKey(this.metricsTopics[i]);
      i++;
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
    this.metricsContextMap = Collections.<String, String>emptyMap();
    processMetricsThreadList = new ArrayList<>();
  }

  public void setMetricsContext(MetricsContext metricsContext) {
    this.metricsContext = metricsContext;
    this.metricsContextMap = metricsContext.getTags();
    metricStore.setMetricsContext(metricsContext);
  }

  final MetricsConsumerMetaTable getMetaTable() {

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

    int i = 0;
    while (i < metricsTopics.length && isRunning()) {
      byte[] messageId = null;
      try {
        messageId = metaTable.getBytes(metricsTopicMetaKeys[i]);
        LOG.info("Last processed MessageId for topic: {} is {}", metricsTopics[i], messageId);
      } catch (Exception e) {
        LOG.info(String.format("Cannot retrieve last processed MessageId for topic: %s", metricsTopics[i]), e);
      }

      MessageFetcher fetcher;
      try {
        fetcher = messagingService.prepareFetch(metricsTopics[i]).setLimit(fetcherLimit);
      } catch (Exception e) {
        LOG.error(String.format("Failed to create fetcher for topic: %s", metricsTopics[i]), e);
        return;
      }
      if (messageId == null) {
        // If no messageId is found for the last processed message, start fetching from beginning
        fetcher.setStartTime(0L);
      } else {
        fetcher.setStartMessage(messageId, false);
      }
      ProcessMetricsThread processMetricsThread = new ProcessMetricsThread(fetcher, metricsTopics[i],
                                                                           metricsTopicMetaKeys[i]);
      processMetricsThread.setDaemon(true);
      processMetricsThread.start();
      processMetricsThreadList.add(processMetricsThread);
      i++;
    }

    for (ProcessMetricsThread thread : processMetricsThreadList) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        LOG.info("Thread {} is being terminated while waiting for it to finish.", thread.getName());
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  protected void shutDown() {
    LOG.info("Stopping Metrics Processing Service.");
    for (ProcessMetricsThread thread : processMetricsThreadList) {
      thread.terminate();
    }
    LOG.info("Metrics Processing Service stopped.");
  }

  @Override
  protected void triggerShutdown() {
    LOG.info("Shutdown is triggered.");
    stopping = true;
    super.triggerShutdown();
  }

  private class ProcessMetricsThread extends Thread {
    private final MessageFetcher fetcher;
    private final TopicIdMetaKey topicIdMetaKey;
    private final List<MetricValues> records;
    byte[] lastMessageId;

    ProcessMetricsThread(MessageFetcher fetcher, TopicId topicId, TopicIdMetaKey topicIdMetaKey) {
      super(String.format("ProcessMetricsThread-%s", topicId.getTopic()));
      this.fetcher = fetcher;
      this.lastMessageId = INITIAL_LAST_MESSAGE_ID;
      this.topicIdMetaKey = topicIdMetaKey;
      records = Lists.newArrayList();
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
          continue;
        }
      }
    }

    public void terminate() {
      LOG.info("Terminate requested {}", getName());
      if (!records.isEmpty()) {
        persistRecords();
      }
      interrupt();
    }

    private void processMetrics() {
      // Decode the metrics records.
      PayloadInputStream is = new PayloadInputStream(new byte[0]);
      BinaryDecoder binaryDecoder = new BinaryDecoder(is);
      try (CloseableIterator<RawMessage> iterator = fetcher.fetch()) {
        while (iterator.hasNext() && isRunning()) {
          RawMessage input = iterator.next();
          try {
            is.reset(input.getPayload());
            MetricValues metricValues = recordReader.read(binaryDecoder, recordSchema);
            records.add(metricValues);
            lastMessageId = input.getId();
            LOG.trace("Received message {} with metrics: {}", lastMessageId, metricValues);
            // persistRecords method persists records and the last messageId if the number of processed records exceeds
            // the persist threshold
          } catch (IOException e) {
            LOG.info("Failed to decode message to MetricValue. Skipped. {}", e.getMessage());
          }
        }
      } catch (Exception e) {
        LOG.error("Failed to fetch metrics records", e);
      }

      if (records.isEmpty()) {
        LOG.debug("No records to process.");
        return;
      }
      persistRecords();
      fetcher.setStartMessage(lastMessageId, false);
    }

    private void persistRecords() {
      try {
        addProcessingStats(records);
        metricStore.add(records);
        LOG.debug("Persisted {} metrics in MetricStore in thread {}", records.size(), this.getName());
        // Persist lastMessageId when its content differs from INITIAL_LAST_MESSAGE_ID
        if (!Arrays.equals(lastMessageId, INITIAL_LAST_MESSAGE_ID)) {
          persistMessageId();
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to add metrics data to a store", e);
      }

      recordsProcessed += records.size();
      // avoid logging more than once a minute
      if (System.currentTimeMillis() > lastLoggedMillis + TimeUnit.MINUTES.toMillis(1)) {
        lastLoggedMillis = System.currentTimeMillis();
        LOG.debug("{} metrics records processed in thread {}. Last record time: {}.",
                 recordsProcessed, this.getName(), records.get(records.size() - 1).getTimestamp());
      }
      records.clear();
    }

    private void addProcessingStats(List<MetricValues> records) {
      if (records.isEmpty()) {
        return;
      }
      int count = records.size();
      long now = System.currentTimeMillis();
      long delay = now - TimeUnit.SECONDS.toMillis(records.get(records.size() - 1).getTimestamp());
      records.add(
        new MetricValues(metricsContextMap, TimeUnit.MILLISECONDS.toSeconds(now),
                         ImmutableList.of(new MetricValue("metrics.process.count", MetricType.COUNTER, count),
                                          new MetricValue("metrics.process.delay.ms", MetricType.GAUGE, delay))));
    }

    private void persistMessageId() {
      try {
        metaTable.save(topicIdMetaKey, lastMessageId);
        LOG.debug("Persisted last processed MessageId: {} in thread {}", lastMessageId, this.getName());
      } catch (Exception e) {
        // Simple log and ignore the error.
        LOG.error("Failed to persist consumed messageId. {}", e.getMessage(), e);
      }
    }
  }

  private class TopicIdMetaKey implements MetricsMetaKey {
    byte[] key;

    TopicIdMetaKey(TopicId metricsTopic) {
      this.key = MessagingUtils.toMetadataRowKey(metricsTopic);
    }

    @Override
    public byte[] getKey() {
      return key;
    }
  }

  private class PayloadInputStream extends ByteArrayInputStream {

    PayloadInputStream(byte[] buf) {
      super(buf);
    }

    void reset(byte[] buf) {
      this.buf = buf;
      this.pos = 0;
      this.count = buf.length;
      this.mark = 0;
    }
  }
}
