package com.continuuity.metrics.process;

import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.data2.OperationException;
import com.continuuity.metrics.data.AggregatesTable;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.continuuity.metrics.transport.MetricsRecord;
import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
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
  private final LoadingCache<String, AggregatesTable> aggregatesTables;

  /**
   * Creates a {@link AggregatesMetricsProcessor} that writes {@link MetricsRecord} to the given
   * aggregates table only if the predicate is passed.
   */
  @Inject
  public AggregatesMetricsProcessor(@Named("metrics.aggregates.predicate") Predicate<MetricsRecord> predicate,
                                    final MetricsTableFactory tableFactory) {
    this.predicate = predicate;
    this.aggregatesTables = CacheBuilder.newBuilder()
                                        .build(new CacheLoader<String, AggregatesTable>() {
      @Override
      public AggregatesTable load(String key) throws Exception {
        return tableFactory.createAggregates(key);
      }
    });
  }

  @Override
  public void process(MetricsScope scope, Iterator<MetricsRecord> records) {
    try {
      aggregatesTables.getUnchecked(scope.name()).update(Iterators.filter(records, predicate));
    } catch (OperationException e) {
      LOG.error("Failed to write to time series table: {}", e.getMessage(), e);
    }
  }
}
