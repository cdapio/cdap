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

package co.cask.cdap.internal.app.runtime.spark.metrics;

import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.internal.app.runtime.spark.SparkContextProvider;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * A {@link ScheduledReporter} reports which reports Metrics collected by the {@link SparkMetricsSink} to
 * {@link MetricsContext}.
 */
final class SparkMetricsReporter extends ScheduledReporter {

  private final MetricsContext metricsContext;

  SparkMetricsReporter(MetricRegistry registry,
                       TimeUnit rateUnit,
                       TimeUnit durationUnit,
                       MetricFilter filter) {
    super(registry, "spark-reporter", filter, rateUnit, durationUnit);
    this.metricsContext = SparkContextProvider.getSparkContext().getMetricsContext();
  }

  /**
   * Called periodically by the polling thread. We are only interested in the Gauges.
   *
   * @param gauges     all of the gauges in the registry
   * @param counters   all of the counters in the registry
   * @param histograms all of the histograms in the registry
   * @param meters     all of the meters in the registry
   * @param timers     all of the timers in the registry
   */
  @Override
  public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
                     SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters,
                     SortedMap<String, Timer> timers) {
    //For now we only use gauge as all the metrics needed are there. We might consider using more in future.
    if (!gauges.isEmpty()) {
      for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
        // for some cases the gauge value is Integer so a typical casting fails. Hence, first cast to Number and then
        // get the value as a long
        String metricName = entry.getKey();
        // NOTE : Stripping uniqueId from the metricName , as we already have runId in context.
        // Currently Spark metric-names are of the format "local-124433533.x.y.z" , we remove local-124433533 and
        // convert this to x.y.z
        String[] metricNameParts = metricName.split("\\.", 2);
        if (metricNameParts.length == 2) {
          metricName = metricNameParts[1];
        }

        long value = ((Number) entry.getValue().getValue()).longValue();
        metricsContext.gauge(metricName, value);
      }
    }
  }
}
