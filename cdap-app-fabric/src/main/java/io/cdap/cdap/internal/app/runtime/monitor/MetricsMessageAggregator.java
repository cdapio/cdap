/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.collect.Iterators;
import com.google.common.reflect.TypeToken;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.common.io.BinaryDecoder;
import io.cdap.cdap.common.io.BinaryEncoder;
import io.cdap.cdap.common.io.DatumReader;
import io.cdap.cdap.common.io.DatumWriter;
import io.cdap.cdap.common.io.StringCachingDecoder;
import io.cdap.cdap.common.utils.ResettableByteArrayInputStream;
import io.cdap.cdap.internal.io.DatumReaderFactory;
import io.cdap.cdap.internal.io.DatumWriterFactory;
import io.cdap.cdap.internal.io.SchemaGenerator;
import io.cdap.cdap.metrics.collect.AggregatedMetricsEmitter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An aggregator for metrics that aggregates metrics on 2 levels.
 * <ol>
 *   <li>Combine metrics across all {@link MetricValues} with the same tags, retaining the latest timestamp.</li>
 *   <li>Inside each {@link MetricValues}, combine {@link MetricValue} with the same name.</li>
 * </ol>
 */
class MetricsMessageAggregator {

  private static final Logger LOG = LoggerFactory.getLogger(
      MetricsMessageAggregator.class);

  private final Map<String, String> cachemap;
  private final ResettableByteArrayInputStream decodingInputStream;
  private final StringCachingDecoder decoder;
  private final DatumReader<MetricValues> reader;
  private final Schema schema;
  private final ByteArrayOutputStream encodeOutputStream;
  private final BinaryEncoder encoder;
  private final DatumWriter<MetricValues> recordWriter;
  private final long aggregationWindowSeconds;

  MetricsMessageAggregator(SchemaGenerator schemaGenerator,
      DatumWriterFactory writerFactory, DatumReaderFactory readerFactory,
      long aggregationWindowSeconds) {
    TypeToken<MetricValues> metricValueType = TypeToken.of(MetricValues.class);
    try {
      this.schema = schemaGenerator.generate(metricValueType.getType());
    } catch (UnsupportedTypeException e) {
      throw new RuntimeException("Failed to generate schema for MetricValues",
          e);
    }
    this.reader = readerFactory.create(metricValueType, schema);
    this.recordWriter = writerFactory.create(metricValueType, schema);
    this.cachemap = new HashMap<>();
    this.decodingInputStream = new ResettableByteArrayInputStream();
    this.decoder = new StringCachingDecoder(
        new BinaryDecoder(decodingInputStream), cachemap);
    this.encodeOutputStream = new ByteArrayOutputStream(1024);
    this.encoder = new BinaryEncoder(encodeOutputStream);
    this.aggregationWindowSeconds = Math.max(1, aggregationWindowSeconds);
  }

  public Iterator<Message> aggregate(Iterator<Message> input) {
    if (!input.hasNext()) {
      return input;
    }
    cachemap.clear();

    Map<MetricsMapKey, MetricsAggregator> aggregated = new HashMap<>();
    long currentTimestamp =
        System.currentTimeMillis() / TimeUnit.SECONDS.toMillis(1);
    // MetricValues will be aggregated across windows of size
    // "aggregationWindowSeconds". We align the current time to the end of the
    // last widow. This is done since in most cases, it is expected that all
    // the MetricValues were emitted very recently. The offset ensures
    // the last window completely lies in the past and maximizes the chance
    // that all metrics lie within it.
    long offset = (currentTimestamp % aggregationWindowSeconds) + 1;
    Message[] lastMessage = new Message[1];
    try {
      input.forEachRemaining(message -> {
        // Decode the metrics.
        // Keep track of the last message ID as TopicRelayers need it to fetch
        // messages in the next iteration.
        lastMessage[0] = message;
        decodingInputStream.reset(message.getPayload());
        MetricValues values;
        try {
          values = reader.read(decoder, schema);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        addToAggregatedValues(aggregated, offset, values);
      });
    } catch (Exception e) {
      LOG.warn(
          "Failed to decode metric message. Returning partially aggregated values.",
          e);
      return Iterators.concat(
          getIteratorFromAggregatedMetrics(aggregated, lastMessage[0].getId()),
          Collections.singleton(lastMessage[0]).iterator(), input);
    }

    return getIteratorFromAggregatedMetrics(aggregated, lastMessage[0].getId());
  }

  private Iterator<Message> getIteratorFromAggregatedMetrics(
      Map<MetricsMapKey, MetricsAggregator> aggregated, String messageId) {
    return aggregated.entrySet().stream().map(this::getMetricValues)
        .map(metricValues -> encodeMetricValues(metricValues, messageId))
        .iterator();
  }

  private void addToAggregatedValues(
      Map<MetricsMapKey, MetricsAggregator> aggregated, long offset,
      MetricValues values) {
    long timestamp = values.getTimestamp();
    long roundedTimestamp = getRoundedTimestamp(timestamp, offset);
    MetricsAggregator metricsAggregator = aggregated.computeIfAbsent(
        new MetricsMapKey(values.getTags(), roundedTimestamp),
        t -> new MetricsAggregator());

    for (MetricValue metric : values.getMetrics()) {
      metricsAggregator.addMetric(metric, timestamp);
    }
  }

  private MetricValues getMetricValues(
      Entry<MetricsMapKey, MetricsAggregator> entry) {
    return entry.getValue().getAggregateMetricValues(
        entry.getKey().getTags(), entry.getKey().getRoundedTimestamp());
  }

  private Message encodeMetricValues(MetricValues metricValues,
      String lastMessageId) {
    encodeOutputStream.reset();
    try {
      recordWriter.encode(metricValues, encoder);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to encode metric to message payload.", e);
    }
    byte[] payload = encodeOutputStream.toByteArray();
    return new Message() {
      @Override
      public String getId() {
        // TopicRelayers keep track of the last message ID
        // to fetch messages in the next iteration.
        // Message IDs are generated by the messaging service.
        // Return the last message ID before aggregation every time.
        return lastMessageId;
      }

      @Override
      public byte[] getPayload() {
        return payload;
      }
    };
  }

  private long getRoundedTimestamp(long timestamp, long offset) {
    return (Math.max(timestamp - offset, 0) / aggregationWindowSeconds)
        * aggregationWindowSeconds;
  }

  /**
   * The key for aggregating {@link MetricValues}. Only values with the same key
   * will be merged.
   */
  private static class MetricsMapKey {

    private final Map<String, String> tags;
    private final long roundedTimestamp;

    private MetricsMapKey(Map<String, String> tags, long roundedTimestamp) {
      this.tags = tags;
      this.roundedTimestamp = roundedTimestamp;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof MetricsMapKey)) {
        return false;
      }
      MetricsMapKey input = (MetricsMapKey) o;
      return roundedTimestamp == input.roundedTimestamp && tags.equals(
          input.tags);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tags, roundedTimestamp);
    }

    public Map<String, String> getTags() {
      return tags;
    }

    public long getRoundedTimestamp() {
      return roundedTimestamp;
    }
  }

  /**
   * Aggregator for {@link MetricValue}s. Users can add {@link MetricValue}s one
   * by one and get the resulting {@link MetricValues} once all the metrics are
   * added.
   */
  private static class MetricsAggregator {

    private final Collection<MetricValue> metrics;
    private final Map<String, AggregatedMetric> aggregatedMetricsMap;

    private MetricsAggregator() {
      this.metrics = new ArrayList<>();
      this.aggregatedMetricsMap = new HashMap<>();
    }

    public void addMetric(MetricValue metric, long timestamp) {
      switch (metric.getType()) {
        case GAUGE:
        case COUNTER:
          aggregatedMetricsMap.computeIfAbsent(metric.getName(),
              AggregatedMetric::new).addMetric(metric, timestamp);
          break;
        default:
          // Don't know how to aggregate other metric types, so store them without aggregation.
          metrics.add(metric);
      }
    }

    public MetricValues getAggregateMetricValues(Map<String, String> tags,
        long timestamp) {
      aggregatedMetricsMap.values().stream()
          .map(AggregatedMetric::emit)
          .forEach(metrics::add);
      return new MetricValues(tags, timestamp, metrics);
    }
  }

  /**
   * A class to obtain an aggregated {@link MetricValue} after adding them one
   * by one.
   */
  private static class AggregatedMetric {

    private final AggregatedMetricsEmitter emitter;
    private long timestamp;

    private AggregatedMetric(String metricName) {
      this.emitter = new AggregatedMetricsEmitter(metricName);
      this.timestamp = 0;
    }

    public void addMetric(MetricValue metric, long timestamp) {
      this.timestamp = Math.max(timestamp, this.timestamp);
      switch (metric.getType()) {
        case GAUGE:
          if (this.timestamp == timestamp) {
            emitter.gauge(metric.getValue());
          }
          break;
        case COUNTER:
          emitter.increment(metric.getValue());
          break;
        default:
          throw new IllegalArgumentException(
              String.format("Can't aggregate metric type %s",
                  metric.getType()));
      }
    }

    public MetricValue emit() {
      return emitter.emit();
    }
  }
}
