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

package co.cask.cdap.metrics.process;

import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.data2.OperationException;
import co.cask.cdap.metrics.data.MetricsTableFactory;
import co.cask.cdap.metrics.data.TimeSeriesTable;
import co.cask.cdap.metrics.transport.MetricsRecord;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * A {@link MetricsProcessor} that writes metrics into time series table. It ignore write errors by simply
 * logging the error and proceed.
 */
public final class TimeSeriesMetricsProcessor implements MetricsProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesMetricsProcessor.class);

  private final LoadingCache<String, List<TimeSeriesTable>> timeSeriesTables;
  private List<MetricsRecord> metricsRecords;

  @Inject
  public TimeSeriesMetricsProcessor(final MetricsTableFactory tableFactory) {
    timeSeriesTables = CacheBuilder.newBuilder()
                                   .build(new CacheLoader<String, List<TimeSeriesTable>>() {
      @Override
      public List<TimeSeriesTable> load(String key) throws Exception {
        LOG.info("Creating Multiple Time Resolution Tables");
        return ImmutableList.of(tableFactory.createTimeSeries(key, 1),
                                tableFactory.createTimeSeries(key, 60),
                                tableFactory.createTimeSeries(key, 3600));
      }
    });
  }

  @Override
  public void process(MetricsScope scope, Iterator<MetricsRecord> records) {
    try {
      List<TimeSeriesTable> listTimeSeriesTables = timeSeriesTables.getUnchecked(scope.name());
      metricsRecords = Lists.newArrayList(records);
      for (TimeSeriesTable table : listTimeSeriesTables) {
        table.save(metricsRecords.iterator());
      }
      metricsRecords.clear();
    } catch (OperationException e) {
      LOG.error("Failed to write to time series table: {}", e.getMessage(), e);
    }
  }
}
