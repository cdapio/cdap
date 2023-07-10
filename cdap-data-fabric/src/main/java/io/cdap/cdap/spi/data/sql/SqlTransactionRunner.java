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

import com.google.common.base.Throwables;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TxRunnable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sql transaction runner will set the transaction isolation level and start a transaction.
 */
public class SqlTransactionRunner implements TransactionRunner {

  private static final Logger LOG = LoggerFactory.getLogger(SqlTransactionRunner.class);

  private final StructuredTableAdmin admin;
  private final DataSource dataSource;
  private final MetricsCollectionService metricsCollectionService;
  private final boolean emitTimeMetrics;
  private final int scanFetchSize;

  public SqlTransactionRunner(StructuredTableAdmin tableAdmin, DataSource dataSource,
      MetricsCollectionService metricsCollectionService,
      boolean emitTimeMetrics, int scanFetchSize) {
    this.admin = tableAdmin;
    this.dataSource = dataSource;
    this.metricsCollectionService = metricsCollectionService;
    this.emitTimeMetrics = emitTimeMetrics;
    this.scanFetchSize = scanFetchSize;
  }

  @Override
  public void run(TxRunnable runnable) throws TransactionException {

    Connection connection;
    try {
      connection = dataSource.getConnection();
    } catch (SQLException e) {
      throw new SqlTransactionException("Unable to get connection to the sql database.", e);
    }

    try {
      MetricsContext metricsCollector = metricsCollectionService.getContext(
          Constants.Metrics.STORAGE_METRICS_TAGS);
      metricsCollector.increment(Constants.Metrics.StructuredTable.TRANSACTION_COUNT, 1L);
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      connection.setAutoCommit(false);
      runnable.run(
          new SqlStructuredTableContext(admin, connection, metricsCollector, emitTimeMetrics,
              this.scanFetchSize));
      connection.commit();
    } catch (Exception e) {
      List<Throwable> causes = Throwables.getCausalChain(e);
      for (Throwable cause : causes) {
        if (cause instanceof SQLException) {
          rollback(connection, new SqlTransactionException((SQLException) cause, e));
          break;
        }
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
