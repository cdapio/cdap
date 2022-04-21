/*
 * Copyright © 2014-2022 Cask Data, Inc.
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

package io.cdap.cdap.metrics.collect;

import com.google.cloud.opentelemetry.metric.MetricConfiguration;
import com.google.cloud.opentelemetry.metric.MetricExporter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import javafx.util.Pair;


import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class OTelMetricsCollectionService extends AbstractExecutionThreadService implements MetricsCollectionService {

  private static final long CACHE_EXPIRE_MINUTES = 1;

  MetricExporter cloudMonitoringExporter;
  MeterProvider provider;
  Meter meter;
  MetricReader reader;

  private final LoadingCache<Map<String, String>, MetricsContext> collectors;
  private final LoadingCache<Map<String, String>, LoadingCache<String, Pair<Attributes, Object>>> emitters;

  // private final LoadingCache<String, LoadingCache<Map<String, String>, Pair<Attributes, Object>>> collectors;

  public OTelMetricsCollectionService() throws IOException {

    // TODO Create exporter outside of CDAP
    cloudMonitoringExporter =
            com.google.cloud.opentelemetry.metric.MetricExporter.createWithConfiguration(
                    MetricConfiguration.builder()
                            // Configure the cloud project id.  Note: this is autodiscovered by default.
                            .setProjectId("carbide-kite-329609")
                            .build()
    );

    // TODO: move this to metricscontext and do it per resource
    // Now set up PeriodicMetricReader to use this Exporter

    // reader = ;
    provider = SdkMeterProvider.builder()
            .registerMetricReader(PeriodicMetricReader.newMetricReaderFactory(cloudMonitoringExporter))
            .build();

    meter = provider.meterBuilder("cdf").build();

    this.collectors = CacheBuilder.newBuilder()
            .expireAfterAccess(CACHE_EXPIRE_MINUTES, TimeUnit.MINUTES)
            .build(createCollectorLoader());


    // context maps to attributes
    this.emitters = CacheBuilder.newBuilder()
            .expireAfterAccess(CACHE_EXPIRE_MINUTES, TimeUnit.MINUTES)
            .build(new CacheLoader<Map<String, String>, LoadingCache<String, Pair<Attributes, Object>>>() {
              @Override
              public LoadingCache<String, Pair<Attributes, Object>> load(Map<String, String> tags) throws Exception {
                return CacheBuilder.newBuilder().expireAfterAccess(CACHE_EXPIRE_MINUTES, TimeUnit.MINUTES).
                        build(new CacheLoader<String, Pair<Attributes, Object>>() {
                          @Override
                          public Pair<Attributes, Object> load(String metricName) throws Exception {
                            AttributesBuilder builder = Attributes.builder();
                            tags.forEach((s1, s2) -> {
                              builder.put(s1, s2);
                            });
                            return new Pair<>(builder.build(), null);
                          }
                        });
              }
            });
  }

  @Override
  protected void run() throws Exception {
    // TODO flush metrics on shutdown
  }

  @Override
  protected void shutDown() throws Exception {
    // Flush the metrics when shutting down.

    // reader.flush();

    cloudMonitoringExporter.flush();
    cloudMonitoringExporter.shutdown();
  }

  @Override
  public MetricsContext getContext(Map<String, String> tags) {

    return collectors.getUnchecked(tags);
  }

  private CacheLoader<Map<String, String>, MetricsContext> createCollectorLoader() {
    return new CacheLoader<Map<String, String>, MetricsContext>() {
      @Override
      public MetricsContext load(final Map<String, String> collectorKey) throws Exception {
        return new OTelMetricsCollectionService.OtelMetricsContextImpl(collectorKey);
      }
    };
  }

  private final class OtelMetricsContextImpl implements MetricsContext {
    // TODO Decide Resource based on context.
    // TODO once MeterProvider and meter creation should happen here based on metrics context

    private final Map<String, String> tags;
    private OtelMetricsContextImpl(final Map<String, String> tags) {
      this.tags = ImmutableMap.copyOf(tags);
    }

    @Override
    public void increment(String metricName, long value) {
      Pair<Attributes, Object> counterPair = emitters.getUnchecked(tags).getUnchecked(metricName);
      LongCounter counter;
      if (counterPair.getValue() == null) {
        // TODO: handle race of setting the counter
        counter = meter.counterBuilder(metricName).setUnit("1").build();
        emitters.getUnchecked(tags).put(metricName, new Pair<>(counterPair.getKey(), counter));
      } else {
        // TODO: check if counterPair.getValue() is LongCounter
        counter = (LongCounter) counterPair.getValue();
      }
      counter.add(value, counterPair.getKey());
    }

    @Override
    public void gauge(String metricName, long value) {
      // TODO: there is no LongGauge in Otel?
      Pair<Attributes, Object> gaugePair = emitters.getUnchecked(tags).getUnchecked(metricName);
      ObservableDoubleGauge gauge;
      if (gaugePair.getValue() == null) {
        gauge = meter.gaugeBuilder(metricName).setUnit("1").buildWithCallback(null);
        emitters.getUnchecked(tags).put(metricName, new Pair<>(gaugePair.getKey(), gauge));
      } else {
        gauge = (ObservableDoubleGauge) gaugePair.getValue();
      }
      // TODO how to set gauge value
    }

    @Override
    public MetricsContext childContext(Map<String, String> tags) {
      return null;
    }

    @Override
    public MetricsContext childContext(String tagName, String tagValue) {
      return null;
    }

    @Override
    public Map<String, String> getTags() {
      return tags;
    }

    @Override
    public void event(String metricName, long value) {
      Pair<Attributes, Object> histogramPair = emitters.getUnchecked(tags).getUnchecked(metricName);
      LongHistogram histogram;
      if (histogramPair.getValue() == null) {
        histogram = meter.histogramBuilder(metricName).setUnit("1").ofLongs().build();
        emitters.getUnchecked(tags).put(metricName, new Pair<>(histogramPair.getKey(), histogram));
      } else {
        histogram = (LongHistogram) histogramPair.getValue();
      }
      histogram.record(value, histogramPair.getKey());
    }
  }
}
