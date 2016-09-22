/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.tephra.metrics.TxMetricsCollector;

/**
 * Implementation for TxMetricsCollector that delegate the the underlying {@link MetricsContext}.
 */
public class TransactionManagerMetricsCollector extends TxMetricsCollector {
  private final MetricsContext metricsContext;

  @Inject
  public TransactionManagerMetricsCollector(MetricsCollectionService service) {
    this.metricsContext = service.getContext(
      ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getEntityName(),
                      Constants.Metrics.Tag.COMPONENT, "transactions"));
  }

  // todo: change TxMetricsCollector in Tephra
  @Override
  public void gauge(String metricName, int value, String...tags) {
    metricsContext.gauge(metricName, value);
  }

  @Override
  public void histogram(String metricName, int value) {
    // TODO: change when CDAP metrics supports histograms: CDAP-3120
    metricsContext.gauge(metricName, value);
  }

  @Override
  public void rate(String metricName) {
    metricsContext.increment(metricName, 1L);
  }

  @Override
  public void rate(String metricName, int count) {
    metricsContext.increment(metricName, count);
  }
}
