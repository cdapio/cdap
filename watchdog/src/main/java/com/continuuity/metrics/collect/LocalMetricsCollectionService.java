/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.collect;

import com.continuuity.metrics.process.MetricsProcessor;
import com.continuuity.metrics.transport.MetricsRecord;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * A {@link com.continuuity.api.metrics.MetricsCollectionService} that writes to MetricsTable directly.
 */
@Singleton
public final class LocalMetricsCollectionService extends AggregatedMetricsCollectionService {

  private final Set<MetricsProcessor> processors;

  @Inject
  public LocalMetricsCollectionService(Set<MetricsProcessor> processors) {
    this.processors = processors;
  }

  @Override
  protected void publish(Iterator<MetricsRecord> metrics) throws Exception {
    List<MetricsRecord> records = ImmutableList.copyOf(metrics);
    for (MetricsProcessor processor : processors) {
      processor.process(records.iterator());
    }
  }
}
