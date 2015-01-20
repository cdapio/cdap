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

import co.cask.cdap.data2.OperationException;
import co.cask.cdap.metrics.data.MetricsTableFactory;
import co.cask.cdap.metrics.data.TimeSeriesTables;
import co.cask.cdap.metrics.transport.MetricsRecord;
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
  private static final int MAX_RECORDLIST_SIZE = 100;

  private final TimeSeriesTables timeSeriesTables;


  @Inject
  public TimeSeriesMetricsProcessor(final MetricsTableFactory tableFactory) {
    this.timeSeriesTables = new TimeSeriesTables(tableFactory);
  }

  @Override
  public void process(Iterator<MetricsRecord> records) {
    try {
      List<MetricsRecord> metricsRecords = Lists.newArrayList();
      while (records.hasNext()) {
        metricsRecords.add(records.next());
        if (metricsRecords.size() == MAX_RECORDLIST_SIZE || !records.hasNext()) {
          timeSeriesTables.save(metricsRecords);
          metricsRecords.clear();
        }
      }
    } catch (OperationException e) {
      LOG.error("Failed to write to time series table: {}", e.getMessage(), e);
    }
  }
}
