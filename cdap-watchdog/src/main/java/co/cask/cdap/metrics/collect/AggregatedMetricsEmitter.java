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

import co.cask.cdap.metrics.transport.MetricType;
import co.cask.cdap.metrics.transport.MetricValue;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final String name;
  private final AtomicLong value;
  private final AtomicBoolean gaugeUsed;

  public AggregatedMetricsEmitter(Map<String, String> tags, String name) {
    if (name == null || name.isEmpty()) {
      LOG.warn("Creating emmitter with " + (name == null ? "null" : "empty") + " name, " +
                 "for context " + Joiner.on(",").withKeyValueSeparator(":").join(tags));
    }

    this.name = name;
    this.value = new AtomicLong();
    this.gaugeUsed = new AtomicBoolean(false);
    this.tags = tags;
  }

  void increment(long value) {
    this.value.addAndGet(value);
  }

  @Override
  public MetricValue emit(long timestamp) {
    long value = this.value.getAndSet(0);
    MetricType type = gaugeUsed.getAndSet(false) ? MetricType.GAUGE : MetricType.COUNTER;
    return new MetricValue(tags, name, timestamp, value, type);
  }

  public void gauge(long value) {
    this.value.set(value);
    this.gaugeUsed.set(true);
  }
}
