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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link MetricsEmitter} that aggregates  values for a metric
 * during collection and emit the aggregated value when emit.
 */
final class AggregatedMetricsEmitter implements MetricsEmitter {
  private static final Logger LOG = LoggerFactory.getLogger(AggregatedMetricsEmitter.class);

  private final String name;
  // metric value
  private final AtomicLong value;
  // specifies if the metric type is gauge or counter
  private final AtomicBoolean gaugeUsed;

  private volatile MetricType metricType = MetricType.COUNTER;
  private AtomicReference<Distribution> distribution = new AtomicReference<>();

  AggregatedMetricsEmitter(String name) {
    if (name == null || name.isEmpty()) {
      LOG.warn("Creating emmitter with " + (name == null ? "null" : "empty") + " name, ");
    }

    this.name = name;
    this.value = new AtomicLong();
    this.gaugeUsed = new AtomicBoolean(false);
  }

  void increment(long value) {
    this.value.addAndGet(value);
  }


  @Override
  public MetricValue emit() {
    // todo CDAP-2195 - potential race condition , reseting value and type has to be done together
    if (metricType == MetricType.DISTRIBUTION) {
      // TODO it is possible the distribution has all 0s. Skip such MetricValues
      Distribution oldVal = distribution.getAndSet(new Distribution());
      LOG.debug(String.format("Emitting distribution metric: %s", distribution.toString()));
      return oldVal.getMetricValue(name);
    } else {
      long value = this.value.getAndSet(0);
      MetricType type = gaugeUsed.getAndSet(false) ? MetricType.GAUGE : MetricType.COUNTER;
      return new MetricValue(name, type, value);
    }
  }

  public void gauge(long value) {
    this.value.set(value);
    this.metricType = MetricType.GAUGE;
  }

  public void event(long value) {
    // TODO
    // 4) MetricValue getAllBucketCounts
    this.metricType = MetricType.DISTRIBUTION;
    synchronized (this) {
      if (distribution.get() == null) {
        distribution.set(new Distribution());
      }
    }
    distribution.get().add(value);
  }
}
