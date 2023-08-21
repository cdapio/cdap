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

package io.cdap.cdap.spi.data.sql;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.transaction.Transactions;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TxRunnable;
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
  // From https://www.postgresql.org/docs/current/errcodes-appendix.html, Class 08 — Connection Exception
  private static final String CONNECTION_EXCEPTION_SQL_STATE_PREFIX = "08";
  private final int maxRetries;
  private final long delayMillisTransactionFailure;
  private final long delayMillisConnectionFailure;

  private final SqlTransactionRunner transactionRunner;
  private final MetricsCollectionService metricsCollectionService;

  @Inject
  public RetryingSqlTransactionRunner(StructuredTableAdmin tableAdmin, DataSource dataSource,
                                      MetricsCollectionService metricsCollectionService, CConfiguration cConf,
                                      int scanFetchSize) {
    this.transactionRunner =
      new SqlTransactionRunner(tableAdmin, dataSource, metricsCollectionService,
                               cConf.getBoolean(Constants.Metrics.STRUCTURED_TABLE_TIME_METRICS_ENABLED),
                               scanFetchSize);
    this.metricsCollectionService = metricsCollectionService;
    this.maxRetries = cConf.getInt(Constants.Dataset.DATA_STORAGE_SQL_TRANSACTION_RUNNER_MAX_RETRIES);
    this.delayMillisTransactionFailure =
      cConf.getLong(Constants.Dataset.DATA_STORAGE_SQL_TRANSACTION_RUNNER_TRANSACTION_FAILURE_DELAY_MILLIS);
    this.delayMillisConnectionFailure =
      cConf.getLong(Constants.Dataset.DATA_STORAGE_SQL_TRANSACTION_RUNNER_CONNECTION_FAILURE_DELAY_MILLIS);
  }

  @Override
  public void run(TxRunnable runnable) throws TransactionException {
    int retries = 0;
    MetricsContext metricsCollector = metricsCollectionService.getContext(Constants.Metrics.STORAGE_METRICS_TAGS);
    while (true) {
      try {
        getSqlTransactionRunner().run(runnable);
        break;
      } catch (SqlTransactionException e) {
        String sqlState = e.getSqlException().getSQLState();
        LOG.trace("Transaction failed with sql state: {}.", sqlState, e);
        // Retry only transaction and connection failure exceptions
        if (TRANSACTION_CONFLICT_SQL_STATE.equals(sqlState)) {
          metricsCollector.increment(Constants.Metrics.StructuredTable.TRANSACTION_CONFLICT, 1L);
          ++retries;
          applyDelay(retries, delayMillisTransactionFailure, e);
        } else if (!Strings.isNullOrEmpty(sqlState) && sqlState.startsWith(CONNECTION_EXCEPTION_SQL_STATE_PREFIX)) {
          LOG.debug("Connection failed with sql state: {}.", sqlState, e);
          ++retries;
          applyDelay(retries, delayMillisConnectionFailure, e);
        } else {
          throw e;
        }
      }
    }
  }

  private void applyDelay(long retryCount, long delayMillis, TransactionException e) throws TransactionException {
    long delay = retryCount > maxRetries ? -1 : delayMillis;
    if (delay < 0) {
      throw e;
    }

    try {
      TimeUnit.MILLISECONDS.sleep(delay);
    } catch (InterruptedException e1) {
      // Reinstate the interrupt thread
      Thread.currentThread().interrupt();
      // Fail with the original exception
      throw e;
    }
  }

  @VisibleForTesting
  public SqlTransactionRunner getSqlTransactionRunner() {
    return transactionRunner;
  }
}
