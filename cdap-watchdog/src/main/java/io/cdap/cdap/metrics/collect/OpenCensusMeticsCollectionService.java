/*
 * Copyright © 2022 Cask Data, Inc.
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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Singleton;
import io.cdap.cdap.api.metrics.MetricType;
import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import io.opencensus.stats.Measure;
import io.opencensus.stats.MeasureMap;
import io.opencensus.stats.Stats;
import io.opencensus.stats.StatsRecorder;
import io.opencensus.stats.ViewManager;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * A MetricsCollectionService writing to OpenCensus (and StackDriver)
 */
@Singleton
public class OpenCensusMeticsCollectionService extends AggregatedMetricsCollectionService{
  private final ViewManager viewManager = Stats.getViewManager();
  private final StatsRecorder statsRecorder = Stats.getStatsRecorder();
  private final LoadingCache<String, Measure.MeasureLong> metricsCache = CacheBuilder
    .newBuilder()
    .build(new CacheLoader<String, Measure.MeasureLong>() {
      @Override
      public Measure.MeasureLong load(String key) throws Exception {
        return loadMeasure(key);
      }
    });

  public OpenCensusMeticsCollectionService(CConfiguration cConf) throws IOException {
    super(TimeUnit.SECONDS.toMillis(cConf.getInt(Constants.Metrics.METRICS_MINIMUM_RESOLUTION_SECONDS)));
    if (cConf.getBoolean(Constants.Metrics.STACKDRIVER_ENABLED, true)) {
      StackdriverStatsExporter.createAndRegister();
    }
    //TODO: Register views
  }

  @Override
  protected void publish(Iterator<MetricValues> metrics) throws Exception {
    while (metrics.hasNext()) {
      MetricValues metricValues = metrics.next();
      MeasureMap measureMap = statsRecorder.newMeasureMap();
      for (MetricValue value: metricValues.getMetrics()) {
        //TODO: Distribution
        if (value.getType() == MetricType.GAUGE || value.getType() == MetricType.COUNTER) {
          measureMap.put(metricsCache.get(value.getName()), value.getValue());
        }
      }
      //TODO: Add tags
      measureMap.record();
    }
  }

  private Measure.MeasureLong loadMeasure(String measureName) {
    return Measure.MeasureLong.create(measureName, measureName, "unit");
  }

}
