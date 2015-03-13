/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction;

import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;

import java.util.Collection;

/**
 * A {@link TransactionAware} that forwards every methods to another {@link TransactionAware}.
 */
public abstract class ForwardingTransactionAware implements TransactionAware {

  private final TransactionAware txAware;

  protected ForwardingTransactionAware(TransactionAware txAware) {
    this.txAware = txAware;
  }

  @Override
  public void startTx(Transaction tx) {
    txAware.startTx(tx);
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    return txAware.getTxChanges();
  }

  @Override
  public boolean commitTx() throws Exception {
    return txAware.commitTx();
  }

  @Override
  public void postTxCommit() {
    txAware.postTxCommit();
  }

  @Override
  public boolean rollbackTx() throws Exception {
    return txAware.rollbackTx();
  }

  @Override
  public String getTransactionAwareName() {
    return txAware.getTransactionAwareName();
  }
}
