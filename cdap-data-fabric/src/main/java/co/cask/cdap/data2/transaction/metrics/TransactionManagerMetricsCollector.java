/*
 * Copyright © 2014-2018 Cask Data, Inc.
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
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.tephra.metrics.TxMetricsCollector;

import java.util.function.Supplier;

/**
 * Implementation for TxMetricsCollector that delegate the the underlying {@link MetricsContext}.
 */
public class TransactionManagerMetricsCollector extends TxMetricsCollector {

  // Lazy initialize the MetricsContext to break circular dependency on
  // LineageWriterDatasetFramework -> BasicLineageWriter
  // -> TransactionSystemClient -> TransactionManager -> this class -> MetricsCollectionService
  private final Supplier<MetricsContext> metricsContext;

  @Inject
  TransactionManagerMetricsCollector(Provider<MetricsCollectionService> serviceProvider) {
    // Take a guice provider instead to delay the instantiation of MetricsCollectionService to break the
    // circular dependency in local mode, usually from
    // <something> -> MetricsCollectionService -> MetricStore -> DatasetFramework -> LineageWriter
    // -> TransactionSystemClient -> TransactionManager
    // -> TransactionManagerMetricsCollector -> MetricsCollectionService
    // However, since TransactionManager is not an interface, Guice cannot proxy it, hence causing error.
    this.metricsContext = new Supplier<MetricsContext>() {
      private volatile MetricsContext context;

      @Override
      public MetricsContext get() {
        MetricsContext context = this.context;
        if (context == null) {
          // The service.getContext already handle concurrent calls, so no need to synchronize here
          this.context = context = serviceProvider.get().getContext(Constants.Metrics.TRANSACTION_MANAGER_CONTEXT);
        }
        return context;
      }
    };
  }

  // todo: change TxMetricsCollector in Tephra
  @Override
  public void gauge(String metricName, int value, String...tags) {
    metricsContext.get().gauge(metricName, value);
  }

  @Override
  public void histogram(String metricName, int value) {
    // TODO: change when CDAP metrics supports histograms: CDAP-3120
    metricsContext.get().gauge(metricName, value);
  }

  @Override
  public void rate(String metricName) {
    metricsContext.get().increment(metricName, 1L);
  }

  @Override
  public void rate(String metricName, int count) {
    metricsContext.get().increment(metricName, count);
  }
}
