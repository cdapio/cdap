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
import co.cask.cdap.api.messaging.TopicNotFoundException;
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
import co.cask.cdap.messaging.data.RawMessage;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import co.cask.common.io.ByteBufferInputStream;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Process metrics by consuming metrics being published to TMS.
 */
public class MessagingMetricsProcessorService extends AbstractMetricsProcessorService {
  private static final Logger LOG = LoggerFactory.getLogger(MessagingMetricsProcessorService.class);

  private static final long INITIAL_LAST_MESSAGE_ID = -1L;

  private final TopicId metricsTopic;
  // TODO unused partitions
  private final Set<Integer> partitions;
  private final MessagingService messagingService;
  private final DatumReader<MetricValues> recordReader;
  private final Schema recordSchema;
  private final MetricStore metricStore;
  private Map<String, String> metricsContextMap;
  private final int persistThreshold;
  private final AtomicInteger messageCount;

  private long lastLoggedMillis;
  private long recordsProcessed;
  private AtomicLong lastMessageId;

  @Inject
  public MessagingMetricsProcessorService(MetricDatasetFactory metricDatasetFactory,
                                          @Named(Constants.Metrics.MESSAGING_TOPIC_PREFIX) String topicPrefix,
                                          @Assisted Set<Integer> partitions,
                                          MessagingService messagingService,
                                          SchemaGenerator schemaGenerator,
                                          DatumReaderFactory readerFactory,
                                          MetricStore metricStore,
                                          @Named(Constants.Metrics.MESSAGING_FETCHER_PERSIST_THRESHOLD)
                                          int persistThreshold) {
    super(metricDatasetFactory);
    this.metricsTopic = NamespaceId.SYSTEM.topic(topicPrefix);
    this.partitions = partitions;
    this.messagingService = messagingService;
    try {
      this.recordSchema = schemaGenerator.generate(MetricValues.class);
      this.recordReader = readerFactory.create(TypeToken.of(MetricValues.class), recordSchema);
    } catch (UnsupportedTypeException e) {
      throw Throwables.propagate(e);
    }
    this.metricStore = metricStore;
    this.persistThreshold = persistThreshold;
    this.messageCount = new AtomicInteger();
    this.lastMessageId = new AtomicLong(INITIAL_LAST_MESSAGE_ID);
    this.metricsContextMap = Collections.<String, String>emptyMap();
  }

  @Override
  public void setMetricsContext(MetricsContext metricsContext) {
    this.metricsContext = metricsContext;
    this.metricsContextMap = metricsContext == null ? Collections.<String, String>emptyMap() : metricsContext.getTags();
  }

  @Override
  protected void run() {
    MessagingConsumerMetaTable metaTable = (MessagingConsumerMetaTable) getMetaTable(this.getClass());
    if (metaTable == null) {
      LOG.info("Could not get MessagingConsumerMetaTable, seems like we are being shut down");
      return;
    }
    try {
      long messageId = metaTable.get(metricsTopic);
      MessageFetcher fetcher = messagingService.prepareFetch(metricsTopic);
      if (messageId == -1L) {
        // If no messageId is found for the last processed message, start fetching from beginning
        fetcher.setStartTime(0L);
      } else {
        fetcher.setStartMessage(Bytes.toBytes(messageId), false);
      }
      while (isRunning()) {
        processMetricsRecords(fetcher);
      }
    } catch (Exception e) {
      LOG.error("Failed to process metrics records", e);
    }
  }

  private void processMetricsRecords(MessageFetcher fetcher) {
    // Decode the metrics records.
    ByteBufferInputStream is = new ByteBufferInputStream(null);
    List<MetricValues> records = Lists.newArrayList();

    try (CloseableIterator<RawMessage> iterator = fetcher.fetch()) {
      while (iterator.hasNext()) {
        RawMessage input = iterator.next();
        try {
          MetricValues metricValues = recordReader.read(
            new BinaryDecoder(is.reset(ByteBuffer.wrap(input.getPayload()))), recordSchema);
          records.add(metricValues);
          lastMessageId.set(Bytes.toLong(input.getId()));
        } catch (IOException e) {
          LOG.info("Failed to decode message to MetricValue. Skipped. {}", e.getMessage());
        }
      }
    } catch (TopicNotFoundException | IOException e) {
      LOG.error("Failed to fetch metrics records", e);
    }

    if (records.isEmpty()) {
      LOG.info("No records to process.");
      return;
    }

    try {
      addProcessingStats(records);
      metricStore.add(records);
      if (messageCount.incrementAndGet() >= persistThreshold) {
        messageCount.set(0);
        persistMessageId();
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to add metrics data to a store", e);
    }

    recordsProcessed += records.size();
    // avoid logging more than once a minute
    if (System.currentTimeMillis() > lastLoggedMillis + TimeUnit.MINUTES.toMillis(1)) {
      lastLoggedMillis = System.currentTimeMillis();
      LOG.info("{} metrics records processed. Last record time: {}.",
               recordsProcessed, records.get(records.size() - 1).getTimestamp());
    }
  }

  @Override
  protected void shutDown() {
    LOG.info("Stopping Metrics Processing Service.");
    // Only save lastMessageId when it differs from its initial value
    if (lastMessageId.get() != INITIAL_LAST_MESSAGE_ID) {
      persistMessageId();
    }
    LOG.info("Metrics Processing Service stopped.");
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
        metaTable.save(metricsTopic, lastMessageId.get());
    } catch (Exception e) {
      // Simple log and ignore the error.
      LOG.error("Failed to persist consumed messageId. {}", e.getMessage(), e);
    }
  }
}
