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
package co.cask.cdap.test.internal;

import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.metrics.collect.AggregatedMetricsCollectionService;
import co.cask.cdap.metrics.process.MetricRecordsWrapper;
import co.cask.cdap.metrics.transport.MetricValue;
import co.cask.cdap.metrics.transport.MetricsRecord;
import co.cask.cdap.test.RuntimeStats;

import java.util.Iterator;

/**
 *
 */
public final class TestMetricsCollectionService extends AggregatedMetricsCollectionService {

  @Override
  protected void publish(MetricsScope scope, Iterator<MetricValue> metrics) throws Exception {
    // Currently the test framework only supports system metrics.
    if (scope != MetricsScope.SYSTEM) {
      return;
    }

    Iterator<MetricsRecord> records = new MetricRecordsWrapper(metrics);
    while (records.hasNext()) {
      MetricsRecord metricsRecord = records.next();
      String context = metricsRecord.getContext();
      // Remove the last part, which is the instance id
      int idx = context.lastIndexOf('.');
      if (idx >= 0) {
        context = context.substring(0, idx);
      }
      RuntimeStats.count(String.format("%s.%s", context, metricsRecord.getName()), metricsRecord.getValue());
    }
  }
}
