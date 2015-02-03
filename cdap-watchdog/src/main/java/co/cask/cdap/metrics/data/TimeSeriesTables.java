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

import co.cask.cdap.data2.OperationException;
import co.cask.cdap.metrics.collect.LocalMetricsCollectionService;
import co.cask.cdap.metrics.transport.MetricsRecord;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Creates Multiple Time Resolution tables and time-series table operations are performed on all the tables.
 */
public class TimeSeriesTables {

  private final LoadingCache<Integer, TimeSeriesTable> metricsTableCaches;
  private final List<Integer> timeSeriesResolutions = ImmutableList.of(1, 60, 3600);
  private static final Logger LOG = LoggerFactory.getLogger(LocalMetricsCollectionService.class);

  public TimeSeriesTables(final MetricsTableFactory metricsTableFactory) {
    this.metricsTableCaches = CacheBuilder.newBuilder().build(new CacheLoader<Integer, TimeSeriesTable>() {
        @Override
        public TimeSeriesTable load(Integer key) throws Exception {
          return metricsTableFactory.createTimeSeries(key);
        }
      });
  }

  public void clear() throws OperationException {
    for (int resolution : timeSeriesResolutions) {
      metricsTableCaches.getUnchecked(resolution).clear();
    }
  }

  public void delete(String contextPrefix, String metricPrefix) throws OperationException {
    for (int resolution : timeSeriesResolutions) {
      metricsTableCaches.getUnchecked(resolution).delete(contextPrefix, metricPrefix);
    }
  }

  public void delete(MetricsScanQuery scanQuery) throws OperationException {
    for (int resolution : timeSeriesResolutions) {
      metricsTableCaches.getUnchecked(resolution).delete(scanQuery);
    }
  }

  public void deleteBefore(long deleteBefore) {
    for (int resolution : timeSeriesResolutions) {
      try {
        metricsTableCaches.getUnchecked(resolution).deleteBefore(deleteBefore);
      } catch (OperationException e) {
        LOG.error("Failed in cleaning up metrics table: {}", e.getMessage(), e);
      }
    }
  }

  public void save(List<MetricsRecord> records) throws Exception {
    for (int resolution : timeSeriesResolutions) {
      metricsTableCaches.getUnchecked(resolution).save(records.iterator());
    }
  }

  public MetricsScanner scan(int resolution, MetricsScanQuery scanQuery) throws OperationException {
    return metricsTableCaches.getUnchecked(resolution).scan(scanQuery);
  }
}
