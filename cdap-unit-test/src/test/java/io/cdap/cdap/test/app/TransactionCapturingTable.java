/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.test.app;

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Table;
import org.apache.tephra.Transaction;

/**
 * A dataset whose only purpose is to remember the transaction when it is started.
 */
public class TransactionCapturingTable extends AbstractDataset {

  private Transaction tx;
  private Table table;

  public TransactionCapturingTable(DatasetSpecification spec, @EmbeddedDataset("t") Table embedded) {
    super(spec.getName(), embedded);
    this.table = embedded;
  }

  @Override
  public void startTx(Transaction tx) {
    super.startTx(tx);
    this.tx = tx;
  }

  @Override
  public void postTxCommit() {
    super.postTxCommit();
    this.tx = null;
  }

  @Override
  public boolean rollbackTx() throws Exception {
    this.tx = null;
    return super.rollbackTx();
  }

  public Transaction getTx() {
    return tx;
  }

  public Table getTable() {
    return table;
  }
}
