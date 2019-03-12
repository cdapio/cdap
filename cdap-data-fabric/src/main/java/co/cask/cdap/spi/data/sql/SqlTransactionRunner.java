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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.transaction.TransactionException;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.spi.data.transaction.TxRunnable;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

/**
 * Sql transaction runner will set the transaction isolation level and start a transaction.
 */
public class SqlTransactionRunner implements TransactionRunner {
  private static final Logger LOG = LoggerFactory.getLogger(SqlTransactionRunner.class);

  private final StructuredTableAdmin admin;
  private final DataSource dataSource;
  private final MetricsCollectionService metricsCollectionService;
  private final boolean emitTimeMetrics;

  @VisibleForTesting
  public SqlTransactionRunner(StructuredTableAdmin admin, DataSource dataSource) {
    this(admin, dataSource, new NoOpMetricsCollectionService(), false);
  }

  public SqlTransactionRunner(StructuredTableAdmin tableAdmin, DataSource dataSource,
                              MetricsCollectionService metricsCollectionService, boolean emitTimeMetrics) {
    this.admin = tableAdmin;
    this.dataSource = dataSource;
    this.metricsCollectionService = metricsCollectionService;
    this.emitTimeMetrics = emitTimeMetrics;
  }

  @Override
  public void run(TxRunnable runnable) throws TransactionException {

    Connection connection;
    try {
      connection = dataSource.getConnection();
    } catch (SQLException e) {
      throw new TransactionException("Unable to get connection to the sql database", e);
    }

    try {
      MetricsContext metricsCollector = metricsCollectionService.getContext(Constants.Metrics.METRICS_TAGS);
      metricsCollector.increment(Constants.Metrics.StructuredTable.TRANSACTION_COUNT, 1L);
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      connection.setAutoCommit(false);
      runnable.run(new SqlStructuredTableContext(admin, connection, metricsCollector, emitTimeMetrics));
      connection.commit();
    } catch (Exception e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        rollback(connection, new SqlTransactionException((SQLException) cause, e));
      }
      rollback(connection, new TransactionException("Failed to execute the sql queries.", e));
    } finally {
      try {
        connection.close();
      } catch (SQLException e) {
        LOG.warn("Failed to close the sql connection after a transaction", e);
      }
    }
  }

  private void rollback(Connection connection, TransactionException e) throws TransactionException {
    try {
      connection.rollback();
    } catch (Exception sql) {
      e.addSuppressed(sql);
    }
    throw e;
  }
}
