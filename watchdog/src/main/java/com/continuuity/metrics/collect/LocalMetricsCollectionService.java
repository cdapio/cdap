/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.collect;

import com.continuuity.metrics.data.MetricsTable;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.continuuity.metrics.transport.MetricsRecord;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.Iterator;

/**
 * A {@link com.continuuity.api.metrics.MetricsCollectionService} that writes to MetricsTable directly.
 */
@Singleton
public final class LocalMetricsCollectionService extends AggregatedMetricsCollectionService {

  private final ThreadLocal<MetricsTable> metricsTable;

  @Inject
  public LocalMetricsCollectionService(final MetricsTableFactory metricsTableFactory) {
    this.metricsTable = new ThreadLocal<MetricsTable>() {
      @Override
      protected MetricsTable initialValue() {
        return metricsTableFactory.create(1);
      }
    };
  }

  @Override
  protected void publish(Iterator<MetricsRecord> metrics) throws Exception {
    metricsTable.get().save(metrics);
  }
}
