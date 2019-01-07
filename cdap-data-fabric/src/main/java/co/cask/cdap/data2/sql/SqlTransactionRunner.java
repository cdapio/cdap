/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.data2.sql;

import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.TransactionRunner;
import co.cask.cdap.spi.data.transaction.TransactionException;
import co.cask.cdap.spi.data.transaction.TxRunnable;

import java.sql.Connection;
import java.sql.SQLException;

/**
 *
 */
public class SqlTransactionRunner implements TransactionRunner {
  private final StructuredTableAdmin admin;
  private final Connection connection;

  public SqlTransactionRunner(StructuredTableAdmin tableAdmin, Connection connection) {
    this.admin = tableAdmin;
    this.connection = connection;
  }

  @Override
  public void run(TxRunnable runnable) throws TransactionException {
    try {
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      connection.setAutoCommit(false);
      runnable.run(new SqlStructuredTableContext(admin, connection));
      connection.commit();
    } catch (Exception e) {
      try {
        connection.rollback();
        throw new TransactionException("Failed to execute sql transaction.", e);
      } catch (SQLException sql) {
        throw new TransactionException("Failed to rollback a transaction.", sql);
      }
    }
  }
}
