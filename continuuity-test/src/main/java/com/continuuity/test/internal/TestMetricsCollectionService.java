/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.test.internal;

import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.metrics.collect.AggregatedMetricsCollectionService;
import com.continuuity.metrics.transport.MetricsRecord;
import com.continuuity.test.RuntimeStats;

import java.util.Iterator;

/**
 *
 */
public final class TestMetricsCollectionService extends AggregatedMetricsCollectionService {

  @Override
  protected void publish(MetricsScope scope, Iterator<MetricsRecord> metrics) throws Exception {
    // Currently the test framework only supports REACTOR metrics.
    if (scope != MetricsScope.REACTOR) {
      return;
    }
    while (metrics.hasNext()) {
      MetricsRecord metricsRecord = metrics.next();
      String context = metricsRecord.getContext();
      // Remove the last part, which is the runID
      context = context.substring(0, context.lastIndexOf('.'));
      RuntimeStats.count(String.format("%s.%s", context, metricsRecord.getName()), metricsRecord.getValue());
    }
  }
}
