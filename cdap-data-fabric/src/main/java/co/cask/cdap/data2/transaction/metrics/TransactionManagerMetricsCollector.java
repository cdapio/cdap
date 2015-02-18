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

package co.cask.cdap.data2.transaction.metrics;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricTags;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.tephra.metrics.TxMetricsCollector;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

/**
 * Implementation for TxMetricsCollector that delegate the the underlying {@link MetricsCollector}.
 */
public class TransactionManagerMetricsCollector extends TxMetricsCollector {
  private final MetricsCollector metricsCollector;

  @Inject
  public TransactionManagerMetricsCollector(MetricsCollectionService service) {
    this.metricsCollector = service.getCollector(
      ImmutableMap.of(MetricTags.NAMESPACE.getCodeName(), Constants.SYSTEM_NAMESPACE,
                      MetricTags.COMPONENT.getCodeName(), "transactions"));
  }

  // todo: change TxMetricsCollector in Tephra
  @Override
  public void gauge(String metricName, int value, String...tags) {
    metricsCollector.increment(metricName, value);
  }

}
