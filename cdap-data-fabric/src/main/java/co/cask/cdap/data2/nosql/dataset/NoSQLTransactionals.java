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

package co.cask.cdap.data2.nosql.dataset;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.data.DatasetContext;
import com.google.common.base.Throwables;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;

/**
 * A utility class for creating a {@link Transactional} to work with entity tables.
 */
public final class NoSQLTransactionals {
  private NoSQLTransactionals() {
    // to prevent instantiation
  }

  /**
   * Create a transactional for an entity table. The regular {@link co.cask.cdap.api.Transactionals} class cannot be
   * used due to cyclic dependency between dataset service and NoSQL StructuredTable.
   *
   * @param txClient transaction client
   * @param datasetSupplier supplies the dataset for the entity table
   * @return transactional for the entity table
   */
  public static Transactional createTransactional(TransactionSystemClient txClient,
                                                  TableDatasetSupplier datasetSupplier) {
    return new Transactional() {
      @Override
      public void execute(co.cask.cdap.api.TxRunnable runnable) throws TransactionFailureException {
        TransactionContext txContext = new TransactionContext(txClient);
        try (EntityTableDatasetContext datasetContext = new EntityTableDatasetContext(txContext, datasetSupplier)) {
          txContext.start();
          finishExecute(txContext, datasetContext, runnable);
        } catch (Exception e) {
          Throwables.propagateIfPossible(e, TransactionFailureException.class);
        }
      }

      @Override
      public void execute(int timeout, co.cask.cdap.api.TxRunnable runnable) throws TransactionFailureException {
        TransactionContext txContext = new TransactionContext(txClient);
        try (EntityTableDatasetContext datasetContext = new EntityTableDatasetContext(txContext, datasetSupplier)) {
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
}
