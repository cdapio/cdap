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
