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
import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * {@link Iterator} for {@link MetricValue} that maintains {@link GaugeStats} for the visited metrics.
 */
public class MetricsIterator extends AbstractIterator<MetricValue> {

  private final GaugeStats processDelayStats;
  private final Iterator<MetricValue> rawMetricsItor;

  public MetricsIterator(Iterator<MetricValue> rawMetricsItor) {
    this.rawMetricsItor = rawMetricsItor;
    this.processDelayStats = new GaugeStats();
  }

  @Override
  protected MetricValue computeNext() {
    if (rawMetricsItor.hasNext()) {
      MetricValue metric = rawMetricsItor.next();
      processDelayStats.gauge(metric.getTimestamp());
      return metric;
    } else {
      return endOfData();
    }
  }

  public Iterator<MetricValue> getMetaMetrics() {
    if (processDelayStats.isEmpty()) {
      return Iterators.emptyIterator();
    }

    long currentTimeMs = System.currentTimeMillis();
    long currentTimeSec = TimeUnit.MILLISECONDS.toSeconds(currentTimeMs);

    MetricValue delayAvg = new MetricValue(
      ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, "system"),
      "metrics.global.processed.delay.avg", currentTimeSec,
      currentTimeMs - TimeUnit.SECONDS.toMillis(processDelayStats.getAverage()), MetricType.GAUGE);

    MetricValue delayMin = new MetricValue(
      ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, "system"),
      "metrics.global.processed.delay.min", currentTimeSec,
      currentTimeMs - TimeUnit.SECONDS.toMillis(processDelayStats.getMin()), MetricType.GAUGE);

    MetricValue delayMax = new MetricValue(
      ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, "system"),
      "metrics.global.processed.delay.max", currentTimeSec,
      currentTimeMs - TimeUnit.SECONDS.toMillis(processDelayStats.getMax()), MetricType.GAUGE);

    MetricValue count = new MetricValue(
      ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, "system"),
      "metrics.global.processed.count", currentTimeSec, processDelayStats.getCount(), MetricType.COUNTER);

     return ImmutableList.of(delayAvg, delayMin, delayMax, count).iterator();
  }
}
