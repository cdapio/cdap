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
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.transaction.TransactionException;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.spi.data.transaction.TxRunnable;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;

import java.util.Collections;

/**
 * No sql transaction runner to start a transaction
 */
public class NoSqlTransactionRunner implements TransactionRunner {
  private final StructuredTableAdmin tableAdmin;
  private final Transactional transactional;

  public NoSqlTransactionRunner(DatasetFramework dsFramework, TransactionSystemClient txClient) {
    this.tableAdmin = new NoSqlStructuredTableAdmin(dsFramework);
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(dsFramework), new TransactionSystemClientAdapter(txClient),
        NamespaceId.SYSTEM, Collections.emptyMap(), null, null)),
      org.apache.tephra.RetryStrategies.retryOnConflict(20, 100)
    );
  }

  @Override
  public void run(TxRunnable runnable) throws TransactionException {
    try {
      transactional.execute(
        datasetContext -> runnable.run(new NoSqlStructuredTableContext(datasetContext, tableAdmin))
      );
    } catch (TransactionFailureException e) {
      throw new TransactionException("Failure executing NoSql transaction:", e.getCause() == null ? e : e.getCause());
    }
  }
}
