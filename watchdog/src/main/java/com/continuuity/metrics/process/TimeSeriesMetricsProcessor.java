/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.metrics.process;

import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.data2.OperationException;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.continuuity.metrics.data.TimeSeriesTable;
import com.continuuity.metrics.transport.MetricsRecord;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * A {@link MetricsProcessor} that writes metrics into time series table. It ignore write errors by simply
 * logging the error and proceed.
 */
public final class TimeSeriesMetricsProcessor implements MetricsProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesMetricsProcessor.class);

  private final LoadingCache<String, TimeSeriesTable> timeSeriesTables;

  @Inject
  public TimeSeriesMetricsProcessor(final MetricsTableFactory tableFactory) {
    timeSeriesTables = CacheBuilder.newBuilder()
                                   .build(new CacheLoader<String, TimeSeriesTable>() {
      @Override
      public TimeSeriesTable load(String key) throws Exception {
        return tableFactory.createTimeSeries(key, 1);
      }
    });
  }

  @Override
  public void process(MetricsScope scope, Iterator<MetricsRecord> records) {
    try {
      timeSeriesTables.getUnchecked(scope.name()).save(records);
    } catch (OperationException e) {
      LOG.error("Failed to write to time series table: {}", e.getMessage(), e);
    }
  }
}
