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

package co.cask.cdap.metrics.data;

import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.data2.OperationException;
import co.cask.cdap.metrics.collect.LocalMetricsCollectionService;
import co.cask.cdap.metrics.transport.MetricsRecord;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Creates Multiple Time Resolution tables and time-series table operations are performed on all the tables.
 */
public class TimeSeriesTables {

  private final Map<MetricsScope, LoadingCache<Integer, TimeSeriesTable>> metricsTableCaches;
  private final List<Integer> timeSeriesResolutions = ImmutableList.of(1, 60, 3600);
  private static final Logger LOG = LoggerFactory.getLogger(LocalMetricsCollectionService.class);

  public TimeSeriesTables(final MetricsTableFactory metricsTableFactory) {
    this.metricsTableCaches = Maps.newHashMap();
    for (final MetricsScope scope : MetricsScope.values()) {
      LoadingCache<Integer, TimeSeriesTable> cache =
        CacheBuilder.newBuilder().build(new CacheLoader<Integer, TimeSeriesTable>() {
          @Override
          public TimeSeriesTable load(Integer key) throws Exception {
            return metricsTableFactory.createTimeSeries(scope.name(), key);
          }
        });
      this.metricsTableCaches.put(scope, cache);
    }
  }

  public void clear(MetricsScope scope) throws OperationException {
    for (int resolution : timeSeriesResolutions) {
      metricsTableCaches.get(scope).getUnchecked(resolution).clear();
    }
  }

  public void delete(MetricsScope scope, String contextPrefix, String metricPrefix) throws OperationException {
    for (int resolution : timeSeriesResolutions) {
      metricsTableCaches.get(scope).getUnchecked(resolution).delete(contextPrefix, metricPrefix);
    }
  }

  public void delete(MetricsScope scope, MetricsScanQuery scanQuery) throws OperationException {
    for (int resolution : timeSeriesResolutions) {
      metricsTableCaches.get(scope).getUnchecked(resolution).delete(scanQuery);
    }
  }

  public void deleteBefore(MetricsScope scope, long deleteBefore) {
    for (int resolution : timeSeriesResolutions) {
      try {
        metricsTableCaches.get(scope).getUnchecked(resolution).deleteBefore(deleteBefore);
      } catch (OperationException e) {
        LOG.error("Failed in cleaning up metrics table: {}", e.getMessage(), e);
      }
    }
  }

  public void save(MetricsScope scope, List<MetricsRecord> records) throws OperationException {
    for (int resolution : timeSeriesResolutions) {
      metricsTableCaches.get(scope).getUnchecked(resolution).save(records.iterator());
    }
  }

  public MetricsScanner scan(MetricsScope scope, int resolution, MetricsScanQuery scanQuery) throws OperationException {
    return metricsTableCaches.get(scope).getUnchecked(resolution).scan(scanQuery);
  }
}
