/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.spi.data.nosql;

import com.google.inject.Inject;
import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.transaction.Transactions;
import io.cdap.cdap.spi.data.nosql.dataset.NoSQLTransactionals;
import io.cdap.cdap.spi.data.nosql.dataset.TableDatasetSupplier;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TxRunnable;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;

import java.io.IOException;
import java.util.Map;

/**
 * No sql transaction runner to start a transaction
 */
public class NoSqlTransactionRunner implements TransactionRunner {
  private final NoSqlStructuredTableAdmin tableAdmin;
  private final Transactional transactional;
  private final MetricsCollectionService metricsCollectionService;
  private final boolean emitTimeMetrics;

  @Inject
  public NoSqlTransactionRunner(NoSqlStructuredTableAdmin tableAdmin, TransactionSystemClient txClient,
                                MetricsCollectionService metricsCollectionService, CConfiguration cConf) {
    this.tableAdmin = tableAdmin;
    this.transactional = Transactions.createTransactionalWithRetry(
      NoSQLTransactionals.createTransactional(txClient, new TableDatasetSupplier() {
        @Override
        public <T extends Dataset> T getTableDataset(String name, Map<String, String> arguments) throws IOException {
          return tableAdmin.getEntityTable(arguments);
        }
      }),
      RetryStrategies.retryOnConflict(20, 100));
    this.metricsCollectionService = metricsCollectionService;
    this.emitTimeMetrics = cConf.getBoolean(Constants.Metrics.STRUCTURED_TABLE_TIME_METRICS_ENABLED);
  }

  @Override
  public void run(TxRunnable runnable) throws TransactionException {
    try {
      MetricsContext metricsCollector = metricsCollectionService.getContext(Constants.Metrics.STORAGE_METRICS_TAGS);
      transactional.execute(
        datasetContext -> runnable.run(new NoSqlStructuredTableContext(tableAdmin, datasetContext,
                                                                       metricsCollector, emitTimeMetrics))
      );
    } catch (TransactionFailureException e) {
      throw new TransactionException("Failure executing NoSql transaction:", e.getCause() == null ? e : e.getCause());
    }
  }
}
