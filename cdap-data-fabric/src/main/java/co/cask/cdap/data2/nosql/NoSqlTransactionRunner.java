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

package co.cask.cdap.data2.nosql;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.data2.nosql.dataset.NoSQLTransactionals;
import co.cask.cdap.data2.nosql.dataset.TableDatasetSupplier;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.spi.data.transaction.TransactionException;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.spi.data.transaction.TxRunnable;
import com.google.inject.Inject;
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

  @Inject
  public NoSqlTransactionRunner(NoSqlStructuredTableAdmin tableAdmin, TransactionSystemClient txClient) {
    this.tableAdmin = tableAdmin;
    this.transactional = Transactions.createTransactionalWithRetry(
      NoSQLTransactionals.createTransactional(txClient, new TableDatasetSupplier() {
        @Override
        public <T extends Dataset> T getTableDataset(String name, Map<String, String> arguments) throws IOException {
          return tableAdmin.getEntityTable(arguments);
        }
      }),
      RetryStrategies.retryOnConflict(20, 100));
  }

  @Override
  public void run(TxRunnable runnable) throws TransactionException {
    try {
      transactional.execute(
        datasetContext -> runnable.run(new NoSqlStructuredTableContext(tableAdmin, datasetContext))
      );
    } catch (TransactionFailureException e) {
      throw new TransactionException("Failure executing NoSql transaction:", e.getCause() == null ? e : e.getCause());
    }
  }
}
