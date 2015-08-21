/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.internal.io.DatumReader;
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
  private long recordProcessed;
  private final MetricStore metricStore;
  private final Map<String, String> metricsContext;

  public MetricsMessageCallback(DatumReader<MetricValues> recordReader,
                                Schema recordSchema,
                                MetricStore metricStore,
                                @Nullable
                                MetricsContext metricsContext) {
    this.recordReader = recordReader;
    this.recordSchema = recordSchema;
    this.metricStore = metricStore;
    this.metricsContext = metricsContext == null ? Collections.<String, String>emptyMap() : metricsContext.getTags();
  }

  @Override
  public void onReceived(Iterator<FetchedMessage> messages) {
    // Decode the metrics records.
    final ByteBufferInputStream is = new ByteBufferInputStream(null);
    List<MetricValues> records = Lists.newArrayList();

    while (messages.hasNext()) {
      FetchedMessage input = messages.next();
      try {
        MetricValues metricValues = recordReader.read(new BinaryDecoder(is.reset(input.getPayload())), recordSchema);
        records.add(metricValues);
      } catch (IOException e) {
        LOG.info("Failed to decode message to MetricValue. Skipped. {}", e.getMessage());
      }
    }

    if (records.isEmpty()) {
      LOG.info("No records to process.");
      return;
    }

    try {
      addProcessingStats(records);
      metricStore.add(records);
    } catch (Exception e) {
      String msg = "Failed to add metrics data to a store";
      LOG.error(msg);
      // todo: will it shut down the whole the metrics processor service??
      throw new RuntimeException(msg, e);
    }

    recordProcessed += records.size();
    if (recordProcessed % 1000 == 0) {
      LOG.info("{} metrics records processed", recordProcessed);
      LOG.info("Last record time: {}", records.get(records.size() - 1).getTimestamp());
    }
  }

  private void addProcessingStats(List<MetricValues> records) {
    if (records.isEmpty()) {
      return;
    }
    int count = records.size();
    long now = System.currentTimeMillis();
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
