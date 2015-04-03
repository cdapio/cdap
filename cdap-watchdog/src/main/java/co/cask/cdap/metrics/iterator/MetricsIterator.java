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

package co.cask.cdap.metrics.iterator;

import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.MetricValue;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.metrics.stats.GaugeStats;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * {@link Iterator} for {@link MetricValue} that maintains {@link GaugeStats} for the visited metrics.
 */
public class MetricsIterator extends AbstractIterator<MetricValue> {

  private final GaugeStats stats;
  private final Iterator<MetricValue> rawMetricsItor;

  public MetricsIterator(Iterator<MetricValue> rawMetricsItor) {
    this.rawMetricsItor = rawMetricsItor;
    this.stats = new GaugeStats();
  }

  @Override
  protected MetricValue computeNext() {
    if (rawMetricsItor.hasNext()) {
      MetricValue metric = rawMetricsItor.next();
      stats.gauge(metric.getValue());
      return metric;
    } else {
      return endOfData();
    }
  }

  public Iterator<MetricValue> getMetaMetrics(long timestampMs) {
    long timestamp = TimeUnit.MILLISECONDS.toSeconds(timestampMs);

    MetricValue delayAvg = new MetricValue(
      ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, "system"),
      "metrics.global.processed.delay.avg", timestamp, (long) stats.getAverage(), MetricType.GAUGE);

    MetricValue delayMin = new MetricValue(
      ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, "system"),
      "metrics.global.processed.delay.min", timestamp, stats.getMin(), MetricType.GAUGE);

    MetricValue delayMax = new MetricValue(
      ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, "system"),
      "metrics.global.processed.delay.max", timestamp, stats.getMax(), MetricType.GAUGE);

    MetricValue count = new MetricValue(
      ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, "system"),
      "metrics.global.processed.count", timestamp, stats.getCount(), MetricType.COUNTER);

    return ImmutableList.of(delayAvg, delayMin, delayMax, count).iterator();
  }
}
