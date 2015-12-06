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

import co.cask.cdap.api.data.DatasetProvider;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.data2.dataset2.SingleThreadDatasetCache;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import javax.annotation.Nullable;

/**
 * This is an implementation of TransactionContext that delays the dismissal of a transaction-aware
 * dataset until after the transaction is complete. This is needed in cases where a client calls
 * {@link DatasetProvider#dismissDataset(Dataset)} in the middle of a transaction: The client indicates
 * that it does not need that dataset any more. But it is participating in the current transaction,
 * and needs to continue to do so until the transaction has ended. Therefore this class will put
 * that dataset on a toDismiss set, which is inspected after every transaction.
 */
public class DelayedDismissingTransactionContext extends TransactionContext {

  private static final Logger LOG = LoggerFactory.getLogger(DelayedDismissingTransactionContext.class);

  private final TransactionSystemClient txClient;
  private final Collection<TransactionAware> txAwares;
  private final Collection<TransactionAware> toDismiss;
  private final SingleThreadDatasetCache cache;
  private TransactionContext txContext;

  /**
   * Constructs the context from the transaction system client (needed by TransactionContext) and
   * the dataset cache that owns this context. That cache is needed to close and dismiss datasets
   * after a transaction has finished.
   * @param txClient the transaction system client, passed on to the actual transaction context
   * @param cache the dataset cache that owns this context, and that will close and invalidate each dataset
   */
  public DelayedDismissingTransactionContext(TransactionSystemClient txClient, SingleThreadDatasetCache cache) {
    super(txClient);
    this.txClient = txClient;
    this.txAwares = Sets.newIdentityHashSet();
    this.toDismiss = Sets.newIdentityHashSet();
    this.cache = cache;
  }

  @Override
  public boolean addTransactionAware(TransactionAware txAware) {
    if (!txAwares.add(txAware)) {
      return false; // it must already be in the actual tx-context
    }
    // this is new, add it to current tx context
    if (txContext != null) {
      txContext.addTransactionAware(txAware);
    }
    // in case this was marked for dismissal, remove that mark
    toDismiss.remove(txAware);
    return true;
  }

  @Override
  public boolean removeTransactionAware(TransactionAware txAware) {
    // if the actual tx-context is non-null, we are in the middle of a transaction, and can't remove the tx-aware
    // so just remove this from the tx-awares here, and the next transaction will be started without it.
    return txAwares.remove(txAware);
  }

  /**
   * Mark a tx-aware for dismissal after the transaction is complete.
   */
  public void dismissAfterTx(TransactionAware txAware) {
    toDismiss.add(txAware);
    txAwares.remove(txAware);
  }

  /**
   * Dismisses all datasets marked for dismissal, through the dataset cache, and set the tx context to null.
   */
  public void cleanup() {
    for (TransactionAware txAware : toDismiss) {
      cache.dismissSafely(txAware);
    }
    toDismiss.clear();
    txContext = null;
  }

  @Override
  public void start() throws TransactionFailureException {
    if (txContext != null && txContext.getCurrentTransaction() != null) {
      LOG.warn("Starting a new transaction while the previous transaction {} is still on-going. ",
               txContext.getCurrentTransaction().getTransactionId());
      cleanup();
    }
    txContext = new TransactionContext(txClient, txAwares);
    txContext.start();
  }

  @Override
  public void finish() throws TransactionFailureException {
    // copied from TransactionContext so it behaves exactly the same in this case
    Preconditions.checkState(txContext != null, "Cannot finish tx that has not been started");
    try {
      txContext.finish();
    } finally {
      cleanup();
    }
  }

  @Override
  public void checkpoint() throws TransactionFailureException {
    // copied from TransactionContext so it behaves exactly the same in this case
    Preconditions.checkState(txContext != null, "Cannot checkpoint tx that has not been started");
    txContext.checkpoint();
  }

  @Nullable
  @Override
  public Transaction getCurrentTransaction() {
    return txContext == null ? null : txContext.getCurrentTransaction();
  }

  @Override
  public void abort(TransactionFailureException cause) throws TransactionFailureException {
    if (txContext == null) {
      // same behavior as Tephra's TransactionContext
      // might be called by some generic exception handler even though already aborted/finished - we allow that
      return;
    }
    try {
      txContext.abort(cause);
    } finally {
      cleanup();
    }
  }
}
