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
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

/**
 * {@link Iterator} for {@link MetricValue} that adds the following meta metrics:
 *   - Processing delay in milliseconds per metric, at most once a second
 *   - Total number of metrics processed
 */
public class IteratorWithMetaMetrics extends AbstractIterator<MetricValue> {

  private static final long SAMPLING_DELAY_SEC = 1;

  private final Iterator<MetricValue> rawMetricsItor;

  private final Queue<MetricValue> pendingMetrics = new LinkedList<MetricValue>();
  private boolean queuedAggregateMetrics = false;
  private long timestampLastSampled;
  private int numMetrics = 0;

  public IteratorWithMetaMetrics(Iterator<MetricValue> rawMetricsItor) {
    this.rawMetricsItor = rawMetricsItor;
  }

  @Override
  protected MetricValue computeNext() {
    if (rawMetricsItor.hasNext()) {
      if (pendingMetrics.isEmpty()) {
        // publish raw metric
        MetricValue raw = rawMetricsItor.next();
        numMetrics++;

        // sample processing delay - assumes we mostly receive metrics in time order
        if (raw.getTimestamp() - timestampLastSampled >= SAMPLING_DELAY_SEC) {
          timestampLastSampled = raw.getTimestamp();

          long timestampMs = TimeUnit.SECONDS.toMillis(raw.getTimestamp());
          long nowMs = System.currentTimeMillis();
          long delayMs = nowMs - timestampMs;
          MetricValue processDelayMetric = new MetricValue(
            ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, "system"),
            "metrics.global.processed.delay.ms", raw.getTimestamp(), delayMs, MetricType.GAUGE);
          pendingMetrics.add(processDelayMetric);
        }

        return raw;
      } else {
        // publish pending metric
        return pendingMetrics.poll();
      }
    } else {
      if (!queuedAggregateMetrics) {
        // looking at last value - queue aggregate metrics if we got anything
        if (numMetrics > 0) {
          MetricValue processedCountMetric = new MetricValue(
            ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, "system"),
            "metrics.global.processed.count", timestampLastSampled, numMetrics, MetricType.COUNTER);
          pendingMetrics.add(processedCountMetric);
        }
        queuedAggregateMetrics = true;
      }

      if (!pendingMetrics.isEmpty()) {
        // publish an aggregate metric
        return pendingMetrics.poll();
      } else {
        // done publishing aggregate metrics
        return endOfData();
      }
    }
  }

}
