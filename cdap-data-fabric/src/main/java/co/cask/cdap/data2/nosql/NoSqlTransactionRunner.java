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
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.spi.data.transaction.TransactionException;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.spi.data.transaction.TxRunnable;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionContext;
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
      createTransactional(txClient, tableAdmin::getEntityTable),
      RetryStrategies.retryOnConflict(20, 100));
  }

  static Transactional createTransactional(TransactionSystemClient txClient, TableDatasetAccesor datasetAccesor) {
    return new Transactional() {
      @Override
      public void execute(co.cask.cdap.api.TxRunnable runnable) throws TransactionFailureException {
        TransactionContext txContext = new TransactionContext(txClient);
        try (EntityTableDatasetContext datasetContext = new EntityTableDatasetContext(txContext, datasetAccesor)) {
          txContext.start();
          finishExecute(txContext, datasetContext, runnable);
        } catch (Exception e) {
          Throwables.propagateIfPossible(e, TransactionFailureException.class);
        }
      }

      @Override
      public void execute(int timeout, co.cask.cdap.api.TxRunnable runnable) throws TransactionFailureException {
        TransactionContext txContext = new TransactionContext(txClient);
        try (EntityTableDatasetContext datasetContext = new EntityTableDatasetContext(txContext, datasetAccesor)) {
          txContext.start(timeout);
          finishExecute(txContext, datasetContext, runnable);
        } catch (Exception e) {
          Throwables.propagateIfPossible(e, TransactionFailureException.class);
        }
      }

      private void finishExecute(TransactionContext txContext, DatasetContext dsContext,
                                 co.cask.cdap.api.TxRunnable runnable)
        throws TransactionFailureException {
        try {
          runnable.run(dsContext);
        } catch (Exception e) {
          txContext.abort(new TransactionFailureException("Exception raised from TxRunnable.run() " + runnable, e));
        }
        // The call the txContext.abort above will always have exception thrown
        // Hence we'll only reach here if and only if the runnable.run() returns normally.
        txContext.finish();
      }
    };
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

  private static class EntityTableDatasetContext implements DatasetContext, AutoCloseable {

    private final TransactionContext txContext;
    private final TableDatasetAccesor datasetAccesor;
    private Dataset entityTable = null;

    private EntityTableDatasetContext(TransactionContext txContext, TableDatasetAccesor datasetAccesor) {
      this.txContext = txContext;
      this.datasetAccesor = datasetAccesor;
    }

    @Override
    public void close() throws Exception {
      if (entityTable != null) {
        entityTable.close();
        entityTable = null;
      }
    }

    @Override
    public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
      // this is the only method that gets called on this dataset context (see NoSqlStructuredTableContext)
        if (entityTable == null) {
          try {
            entityTable = datasetAccesor.getTableDataset(name);
            txContext.addTransactionAware((TransactionAware) entityTable);
          } catch (IOException e) {
            throw new DatasetInstantiationException("Cannot instantiate entity table", e);
          }
        }
        //noinspection unchecked
        return (T) entityTable;
    }

    @Override
    public <T extends Dataset> T getDataset(String namespace, String name) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Dataset> T getDataset(String name, Map<String, String> arguments) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Dataset> T getDataset(String namespace, String name, Map<String, String> arguments)  {
      throw new UnsupportedOperationException();
    }

    @Override
    public void releaseDataset(Dataset dataset) {
      // no-op
    }

    @Override
    public void discardDataset(Dataset dataset) {
      // no-op
    }
  }

  interface TableDatasetAccesor {
    <T extends Dataset> T getTableDataset(String name) throws IOException;
  }
}
