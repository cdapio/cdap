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
package io.cdap.cdap.metrics.collect;

import io.cdap.cdap.api.metrics.MetricType;
import io.cdap.cdap.api.metrics.MetricValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link MetricsEmitter} that aggregates  values for a metric during collection and emit the
 * aggregated value when emit.
 */
public final class AggregatedMetricsEmitter implements MetricsEmitter {
  private static final Logger LOG = LoggerFactory.getLogger(AggregatedMetricsEmitter.class);

  private final String name;
  private long value;

  private MetricType metricType = MetricType.COUNTER;
  private Distribution distribution;

  public AggregatedMetricsEmitter(String name) {
    if (name == null || name.isEmpty()) {
      LOG.warn("Creating emmitter with " + (name == null ? "null" : "empty") + " name, ");
    }

    this.name = name;
  }

  public synchronized void increment(long incrementValue) {
    this.value += incrementValue;
    this.metricType = MetricType.COUNTER;
  }

  @Override
  public synchronized MetricValue emit() {
    if (metricType == MetricType.DISTRIBUTION) {
      Distribution oldVal;
      // TODO emit maybe made faster using CAS inside Distribution.
      // https://cdap.atlassian.net/browse/CDAP-18792 has more context
      oldVal = distribution;
      distribution = null;
      if (oldVal != null) {
        LOG.trace("Emitting distribution metric: {}", oldVal.toString());
        return oldVal.getMetricValue(name);
      }
      return new Distribution().getMetricValue(name);
    }

    MetricValue returnVal = new MetricValue(name, metricType, this.value);
    this.value = 0;
    this.metricType = MetricType.COUNTER;
    return returnVal;
  }

  public synchronized void gauge(long value) {
    this.value = value;
    this.metricType = MetricType.GAUGE;
  }

  public synchronized void event(long value) {
    if (distribution == null) {
      distribution = new Distribution();
    }
    distribution.add(value);
    this.metricType = MetricType.DISTRIBUTION;
  }
}
