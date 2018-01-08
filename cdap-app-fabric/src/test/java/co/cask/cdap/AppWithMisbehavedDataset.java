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

package co.cask.cdap;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.worker.AbstractWorker;
import com.google.common.base.Throwables;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionFailureException;

/**
 * Worker at App Level.
 */
public class AppWithMisbehavedDataset extends AbstractApplication {

  public static final String NAME = "AppWithWorker";
  public static final String WORKER = "TableWriter";
  public static final String TABLE = "MyTable";
  public static final String MISBEHAVED = "Misbehaved";
  public static final String ROW = "r";
  public static final String COLUMN = "c";
  public static final String VALUE = "v";

  @Override
  public void configure() {
    setName(NAME);
    addDatasetType(MisbehavedDataset.class);
    addWorker(new TableWriter());
    createDataset(TABLE, Table.class.getName());
    createDataset(MISBEHAVED, MisbehavedDataset.class.getName());
  }

  /**
   * This dataset is used to induce failure in a unit test. After active() is called,
   * both commitTx() and getTransactionAwareName() will throw an exception until the
   * next transaction starts.
   */
  public static class MisbehavedDataset extends AbstractDataset {

    private boolean active = false;

    public MisbehavedDataset(DatasetSpecification spec, @EmbeddedDataset("t") Table table) {
      super(spec.getName(), table);
    }

    public void activate() {
      active = true;
    }

    @Override
    public void startTx(Transaction tx) {
      active = false;
      super.startTx(tx);
    }

    @Override
    public boolean commitTx() throws Exception {
      if (active) {
        throw new DataSetException("misbehaving");
      }
      return super.commitTx();
    }

    @Override
    public String getTransactionAwareName() {
      if (active) {
        throw new DataSetException("misbehaving");
      }
      return super.getTransactionAwareName();
    }
  }

  public class TableWriter extends AbstractWorker {

    @Override
    public void run() {
      try {
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext context) throws Exception {
            MisbehavedDataset misbehaved = context.getDataset(MISBEHAVED);
            misbehaved.activate();
          }
        });
      } catch (Exception e) {
        // expected: activate() makes commitTx() fail
      }
      try {
        // this transaction should go through. If TransactionContext does not handle
        // the exceptions from commitTx() and getTxAwareName(), then we would not be
        // able to start transaction any more: the tx context would still have an
        // active tx and complain about a nested transaction).
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext context) throws Exception {
            Table table = context.getDataset(TABLE);
            table.put(new Put(ROW, COLUMN, VALUE));
          }
        });
      } catch (TransactionFailureException e) {
        Throwables.propagate(e);
      }
    }
  }
}
