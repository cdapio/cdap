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

import co.cask.cdap.metrics.data.AggregatesTable;
import co.cask.cdap.metrics.data.MetricsTableFactory;
import co.cask.cdap.metrics.transport.MetricsRecord;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * A {@link MetricsProcessor} that writes metrics into aggregates table. It ignore write errors by simply
 * logging the error and proceed.
 */
public final class AggregatesMetricsProcessor implements MetricsProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(AggregatesMetricsProcessor.class);

  private final Predicate<MetricsRecord> predicate;
  private final Supplier<AggregatesTable> aggregatesTable;

  /**
   * Creates a {@link AggregatesMetricsProcessor} that writes {@link MetricsRecord} to the given
   * aggregates table only if the predicate is passed.
   */
  @Inject
  public AggregatesMetricsProcessor(@Named("metrics.aggregates.predicate") Predicate<MetricsRecord> predicate,
                                    final MetricsTableFactory tableFactory) {
    this.predicate = predicate;
    this.aggregatesTable = Suppliers.memoize(new Supplier<AggregatesTable>() {
      @Override
      public AggregatesTable get() {
        return tableFactory.createAggregates();
      }
    });
  }

  @Override
  public void process(Iterator<MetricsRecord> records) {
    try {
      aggregatesTable.get().update(Iterators.filter(records, predicate));
    } catch (Exception e) {
      LOG.error("Failed to write to time series table: {}", e.getMessage(), e);
    }
  }
}
