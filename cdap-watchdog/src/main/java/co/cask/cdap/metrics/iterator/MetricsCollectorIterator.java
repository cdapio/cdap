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
import co.cask.cdap.api.metrics.MetricValues;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.metrics.stats.GaugeStats;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * {@link Iterator} for {@link co.cask.cdap.api.metrics.MetricValues} that maintains {@link GaugeStats}
 * for the visited metrics.
 */
public class MetricsCollectorIterator extends AbstractIterator<MetricValues> {

  private final GaugeStats processDelayStats;
  private final Iterator<MetricValues> rawMetricsItor;

  public MetricsCollectorIterator(Iterator<MetricValues> rawMetricsItor) {
    this.rawMetricsItor = rawMetricsItor;
    this.processDelayStats = new GaugeStats();
  }

  @Override
  protected MetricValues computeNext() {
    if (rawMetricsItor.hasNext()) {
      MetricValues metric = rawMetricsItor.next();
      processDelayStats.gauge(metric.getTimestamp());
      return metric;
    } else {
      return endOfData();
    }
  }

  public Iterator<MetricValues> getMetaMetrics() {
    if (processDelayStats.isEmpty()) {
      return Iterators.emptyIterator();
    }

    long currentTimeMs = System.currentTimeMillis();
    long currentTimeSec = TimeUnit.MILLISECONDS.toSeconds(currentTimeMs);

    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, "system",
                                               Constants.Metrics.Tag.COMPONENT, "metrics.processor");

    MetricValues delaySum = new MetricValues(
      tags, "processed.delay.sum", currentTimeSec,
      (currentTimeMs * processDelayStats.getCount()) - TimeUnit.SECONDS.toMillis(processDelayStats.getSum()),
      MetricType.COUNTER);

    MetricValues delayMin = new MetricValues(
      tags, "processed.delay.min", currentTimeSec,
      currentTimeMs - TimeUnit.SECONDS.toMillis(processDelayStats.getMin()), MetricType.GAUGE);

    MetricValues delayMax = new MetricValues(
      tags, "processed.delay.max", currentTimeSec,
      currentTimeMs - TimeUnit.SECONDS.toMillis(processDelayStats.getMax()), MetricType.GAUGE);

    MetricValues count = new MetricValues(
      tags, "processed.count", currentTimeSec,
      processDelayStats.getCount(), MetricType.COUNTER);

     return ImmutableList.of(delaySum, delayMin, delayMax, count).iterator();
  }
}
