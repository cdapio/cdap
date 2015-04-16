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
package co.cask.cdap.metrics.collect;

import co.cask.cdap.api.dataset.lib.cube.MeasureType;
import co.cask.cdap.api.dataset.lib.cube.Measurement;
import co.cask.cdap.api.metrics.MetricValue;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link co.cask.cdap.common.metrics.MetricsCollector} and {@link MetricsEmitter} that aggregates metric values
 * during collection and emit the aggregated values when emit.
 */
final class AggregatedMetricsEmitter implements MetricsEmitter {
  private static final Logger LOG = LoggerFactory.getLogger(AggregatedMetricsEmitter.class);

  private final Map<String, String> tags;
  private Map<String, MockMeasurement> metricNameToMeasurements;


  public AggregatedMetricsEmitter(Map<String, String> tags) {
    this.tags = tags;
    this.metricNameToMeasurements = Maps.newHashMap();
  }

  MockMeasurement getMeasurement(String name) {
    if (!metricNameToMeasurements.containsKey(name)) {
      metricNameToMeasurements.put(name, new MockMeasurement(name));
    }
    return metricNameToMeasurements.get(name);
  }

  void increment(String name, long value) {
    getMeasurement(name).increment(value);
  }

  void gauge(String name, long value) {
    getMeasurement(name).gauge(value);
  }

  @Override
  public MetricValue emit(long timestamp) {
    Collection<Measurement> metrics = Lists.newArrayList();
    for (MockMeasurement metric : metricNameToMeasurements.values()) {
      // skip increment by 0
      if (metric.getMetricType() == MeasureType.COUNTER && metric.getValue() == 0) {
        continue;
      }
      metrics.add(new Measurement(metric.getName(), metric.getMetricType(), metric.getValue()));
      metric.resetValue();
      metric.setGauge(false);
    }
    return new MetricValue(tags, timestamp, metrics);
  }

  class MockMeasurement {
    private final String name;
    private final AtomicLong value;
    private final AtomicBoolean gaugeUsed;

    public MockMeasurement(String name) {
      if (name == null || name.isEmpty()) {
        LOG.warn("Creating emmitter with " + (name == null ? "null" : "empty") + " name, " +
                   "for context " + Joiner.on(",").withKeyValueSeparator(":").join(tags));
      }
      this.name = name;
      this.value = new AtomicLong();
      this.gaugeUsed = new AtomicBoolean(false);
    }

    public String getName() {
      return name;
    }

    public void setGauge(Boolean state) {
      this.gaugeUsed.set(state);
    }

    public void resetValue() {
      this.value.set(0);
    }

    public long getValue() {
      return value.longValue();
    }

    public MeasureType getMetricType() {
      return gaugeUsed.get() == true ? MeasureType.GAUGE : MeasureType.COUNTER;
    }

    public void increment(long value) {
      this.value.addAndGet(value);
    }

    public void gauge(long value) {
      this.value.set(value);
      this.gaugeUsed.set(true);
    }

  }

}
