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

package co.cask.cdap.data2.dataset2.tx;

import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;

import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Collections;

/**
 * An implementation of transaction-aware that forwards all method calls to a delegate. The delegate is held
 * in a weak reference, such that it can be garbage-collected if no other reference to it exists. When that
 * happens, the weak-reference turns to null and all method calls become no-ops. Note that this class (just
 * like {@link WeakReference} itself, does not allow to set or change the weakly-referenced tx-aware other than
 * in the constructor. Hence it is guaranteed that the delegate will always remain the same, or change to null.
 */
public class WeakReferenceDelegatingTransactionAware implements TransactionAware {

  private final WeakReference<TransactionAware> delegateRef;
  private final String txAwareName;

  public WeakReferenceDelegatingTransactionAware(TransactionAware txAware) {
    // remember this so that we can return it after the delegate has expired
    txAwareName = txAware.getTransactionAwareName();
    delegateRef = new WeakReference<>(txAware);
  }

  @Override
  public void startTx(Transaction transaction) {
    TransactionAware txAware = delegateRef.get();
    if (txAware != null) {
      txAware.startTx(transaction);
    }
  }

  @Override
  public void updateTx(Transaction transaction) {
    TransactionAware txAware = delegateRef.get();
    if (txAware != null) {
      txAware.updateTx(transaction);
    }
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    TransactionAware txAware = delegateRef.get();
    if (txAware != null) {
      return txAware.getTxChanges();
    }
    return Collections.emptyList();
  }

  @Override
  public boolean commitTx() throws Exception {
    TransactionAware txAware = delegateRef.get();
    return txAware == null || txAware.commitTx();
  }

  @Override
  public void postTxCommit() {
    TransactionAware txAware = delegateRef.get();
    if (txAware != null) {
      txAware.postTxCommit();
    }
  }

  @Override
  public boolean rollbackTx() throws Exception {
    TransactionAware txAware = delegateRef.get();
    return txAware == null || txAware.rollbackTx();
  }

  @Override
  public String getTransactionAwareName() {
    TransactionAware txAware = delegateRef.get();
    if (txAware != null) {
      return txAware.getTransactionAwareName();
    }
    return txAwareName + "(expired weak reference)";
  }

  public boolean isExpired() {
    return delegateRef.get() == null;
  }

  public TransactionAware get() {
    return delegateRef.get();
  }
}
