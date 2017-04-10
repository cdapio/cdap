/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.MetricValue;
import co.cask.cdap.api.metrics.MetricValues;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.io.BinaryDecoder;
import co.cask.cdap.common.io.DatumReader;
import co.cask.common.io.ByteBufferInputStream;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A {@link KafkaConsumer.MessageCallback} that decodes message into {@link co.cask.cdap.api.metrics.MetricValues}
 * and stores it in {@link MetricStore}.
 */
public final class MetricsMessageCallback implements KafkaConsumer.MessageCallback {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsMessageCallback.class);

  private final DatumReader<MetricValues> recordReader;
  private final Schema recordSchema;
  private final MetricStore metricStore;
  private final Map<String, String> metricsContext;

  private long lastLoggedMillis;
  private long recordsProcessed;

  public MetricsMessageCallback(DatumReader<MetricValues> recordReader,
                                Schema recordSchema,
                                MetricStore metricStore,
                                @Nullable MetricsContext metricsContext) {
    this.recordReader = recordReader;
    this.recordSchema = recordSchema;
    this.metricStore = metricStore;
    this.metricsContext = metricsContext == null ? Collections.<String, String>emptyMap() : metricsContext.getTags();
  }

  @Override
  public long onReceived(Iterator<FetchedMessage> messages) {
    // Decode the metrics records.
    ByteBufferInputStream is = new ByteBufferInputStream(null);
    List<MetricValues> records = Lists.newArrayList();

    long nextOffset = 0L;
    while (messages.hasNext()) {
      FetchedMessage input = messages.next();
      nextOffset = input.getNextOffset();
      try {
        MetricValues metricValues = recordReader.read(new BinaryDecoder(is.reset(input.getPayload())), recordSchema);
        records.add(metricValues);
      } catch (IOException e) {
        LOG.warn("Failed to decode message to MetricValue. Skipped. {}", e.getMessage());
      }
    }

    if (records.isEmpty()) {
      LOG.info("No records to process.");
      return nextOffset;
    }

    long now = System.currentTimeMillis();
    try {
      addProcessingStats(records, now);
      metricStore.add(records);
    } catch (Exception e) {
      // SimpleKafkaConsumer will log the error, and continue on past these messages
      throw new RuntimeException("Failed to add metrics data to a store", e);
    }

    recordsProcessed += records.size();
    // avoid logging more than once a minute
    if (now > lastLoggedMillis + TimeUnit.MINUTES.toMillis(1)) {
      lastLoggedMillis = now;
      LOG.debug("{} metrics records processed. Last record time: {}.",
               recordsProcessed, records.get(records.size() - 1).getTimestamp());
    }
    return nextOffset;
  }

  private void addProcessingStats(List<MetricValues> records, long now) {
    if (records.isEmpty()) {
      return;
    }
    int count = records.size();
    long delay = now - TimeUnit.SECONDS.toMillis(records.get(records.size() - 1).getTimestamp());
    records.add(
      new MetricValues(metricsContext, TimeUnit.MILLISECONDS.toSeconds(now),
                       ImmutableList.of(new MetricValue("metrics.process.count", MetricType.COUNTER, count),
                                        new MetricValue("metrics.process.delay.ms", MetricType.GAUGE, delay))));
  }

  @Override
  public void finished() {
    // Just log
    LOG.info("Metrics MessageCallback completed.");
  }
}
