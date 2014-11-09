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
import co.cask.cdap.metrics.transport.MetricsRecord;
import co.cask.cdap.metrics.transport.TagMetric;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link co.cask.cdap.common.metrics.MetricsCollector} and {@link MetricsEmitter} that aggregates metric values
 * during collection and emit the aggregated values when emit.
 */
final class AggregatedMetricsEmitter implements MetricsEmitter {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(AggregatedMetricsEmitter.class);
  private static final long CACHE_EXPIRE_MINUTES = 1;

  private final String context;
  private final String runId;
  private final String name;
  private final AtomicLong value;
  private final AtomicBoolean gaugeUsed;
  private final LoadingCache<String, AtomicLong> tagValues;

  AggregatedMetricsEmitter(String context, String runId, String name) {
    this.context = context;
    this.runId = runId;
    this.name = name;
    this.value = new AtomicLong();
    this.gaugeUsed = new AtomicBoolean(false);
    this.tagValues = CacheBuilder.newBuilder()
                                 .expireAfterAccess(CACHE_EXPIRE_MINUTES, TimeUnit.MINUTES)
                                 .build(new CacheLoader<String, AtomicLong>() {
                                   @Override
                                   public AtomicLong load(String key) throws Exception {
                                     return new AtomicLong();
                                   }
                                 });
    if (name == null || name.isEmpty()) {
      LOG.warn("Creating emmitter with " + (name == null ? "null" : "empty") + " name, " +
        "for context " + context + " and runId " + runId);
    }
  }

  void increment(int value, String... tags) {
    this.value.addAndGet(value);
    for (String tag : tags) {
      tagValues.getUnchecked(tag).addAndGet(value);
    }
  }

  @Override
  public MetricsRecord emit(long timestamp) {
    ImmutableList.Builder<TagMetric> builder = ImmutableList.builder();
    long value = this.value.getAndSet(0);
    for (Map.Entry<String, AtomicLong> entry : tagValues.asMap().entrySet()) {
      builder.add(new TagMetric(entry.getKey(), entry.getValue().getAndSet(0)));
    }
    MetricType type = gaugeUsed.getAndSet(false) ? MetricType.GAUGE : MetricType.COUNTER;
    return new MetricsRecord(context, runId, name, builder.build(), timestamp, value, type);
  }

  public void gauge(long value, String[] tags) {
    this.value.set(value);
    this.gaugeUsed.set(true);
    for (String tag : tags) {
      tagValues.getUnchecked(tag).set(value);
    }
  }
}
