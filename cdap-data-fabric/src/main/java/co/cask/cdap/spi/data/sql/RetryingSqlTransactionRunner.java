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

package co.cask.cdap.spi.data.sql;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.transaction.TransactionException;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.spi.data.transaction.TxRunnable;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

/**
 * Retries SQL operations in case they fail due to conflict.
 * This class is based on {@link Transactions#createTransactionalWithRetry}.
 */
public class RetryingSqlTransactionRunner implements TransactionRunner {
  private static final Logger LOG = LoggerFactory.getLogger(RetryingSqlTransactionRunner.class);
  // From https://www.postgresql.org/docs/9.6/transaction-iso.html, "40001" is the code for serialization failures
  private static final String TRANSACTION_CONFLICT_SQL_STATE = "40001";
  private static final int MAX_RETRIES = 20;
  private static final long DELAY_MILLIS = 100;

  private final SqlTransactionRunner transactionRunner;
  private final MetricsCollectionService metricsCollectionService;

  @Inject
  public RetryingSqlTransactionRunner(StructuredTableAdmin tableAdmin, DataSource dataSource,
                                      MetricsCollectionService metricsCollectionService, CConfiguration cConf) {
    this.transactionRunner =
      new SqlTransactionRunner(tableAdmin, dataSource, metricsCollectionService,
                               cConf.getBoolean(Constants.Metrics.STRUCTURED_TABLE_TIME_METRICS_ENABLED));
    this.metricsCollectionService = metricsCollectionService;
  }

  @Override
  public void run(TxRunnable runnable) throws TransactionException {
    int retries = 0;
    MetricsContext metricsCollector = metricsCollectionService.getContext(Constants.Metrics.METRICS_TAGS);
    while (true) {
      try {
        transactionRunner.run(runnable);
        break;
      } catch (SqlTransactionException e) {
        String sqlState = e.getSqlException().getSQLState();
        LOG.trace("Transaction failed with sql state: {}.", sqlState, e);
        // Retry only transaction failure exceptions
        if (TRANSACTION_CONFLICT_SQL_STATE.equals(sqlState)) {
          metricsCollector.increment(Constants.Metrics.StructuredTable.TRANSACTION_CONFLICT, 1L);
          ++retries;
          long delay = retries > MAX_RETRIES ? -1 : DELAY_MILLIS;
          if (delay < 0) {
            throw e;
          }

          if (delay > 0) {
            try {
              TimeUnit.MILLISECONDS.sleep(delay);
            } catch (InterruptedException e1) {
              // Reinstate the interrupt thread
              Thread.currentThread().interrupt();
              // Fail with the original exception
              throw e;
            }
          }
        } else {
          throw e;
        }
      }
    }
  }
}
