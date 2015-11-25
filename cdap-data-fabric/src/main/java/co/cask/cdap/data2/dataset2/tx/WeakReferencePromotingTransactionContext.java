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
 * This is an implementation of TransactionContext that maintains some of its transaction-awares as
 * weak references, to allow them to be garbage-collected between transactions:
 * <ul>
 *   <li>When a new transaction is started, it promotes all weak references to strong references
 *   (by de-referencing before adding them to the transaction);</li>
 *   <li>When the transaction finishes (either through finish() or abort()), it discards all strong
 *   references;</li>
 *   <li>All other transaction-awares (not instances of {@link WeakReferenceDelegatingTransactionAware})
 *   are passed through to the transaction as is.</li>
 * </ul>
 * This makes sure that after the transaction is finished, the weak-references can be garbage-collected. But
 * for the lifetime of a transaction, they are protected from garbage collection to ensure that they can
 * participate in commit or rollback.
 *
 * Note that because TransactionContext is not an interface, we must extend the existing implementation
 * even though this implementation in fact delegates to another TransactionContext rather than extending one.
 */
public class WeakReferencePromotingTransactionContext extends TransactionContext {

  private static final Logger LOG = LoggerFactory.getLogger(WeakReferencePromotingTransactionContext.class);

  private final TransactionSystemClient txClient;
  private final Collection<TransactionAware> txAwares;
  private TransactionContext txContext;

  public WeakReferencePromotingTransactionContext(TransactionSystemClient txClient) {
    super(txClient);
    this.txClient = txClient;
    this.txAwares = Sets.newLinkedHashSet();
  }

  @Override
  public boolean addTransactionAware(TransactionAware txAware) {
    TransactionAware toAdd = txAware;
    // promote weak references to the actual tx-aware before adding to actual tx-context
    if (txAware instanceof WeakReferenceDelegatingTransactionAware) {
      toAdd = ((WeakReferenceDelegatingTransactionAware) txAware).get();
      if (toAdd == null) {
        // this should never happen. It means that a tx-aware was already gc'ed before it was added here
        LOG.warn("Attempt to add weak-referenced transaction-aware '{}' that has already been garbage-collected");
        return false;
      }
    }
    if (!txAwares.add(txAware)) {
      return false; // it must already be in the actual tx-context
    }
    if (txContext != null && !txContext.addTransactionAware(toAdd) && toAdd != txAware) {
      // this must be a duplicate weak-reference to the same tx-aware, which should never happen
      LOG.warn("Attempt to add weak-referenced transaction-aware '{}' that was already added " +
                 "either directly or through a different weak reference");
    }
    return true;
  }

  @Override
  public boolean removeTransactionAware(TransactionAware txAware) {
    // if the actual tx-context is non-null, we are in the middle of a transaction, and can't remove the tx-aware
    // so just remove this from the tx-awares here, and the next transaction will be started without it.
    return txAwares.remove(txAware);
  }

  @Override
  public void start() throws TransactionFailureException {
    if (txContext != null && txContext.getCurrentTransaction() != null) {
      LOG.warn("Starting a new transaction while the previous transaction {} is still on-going. ",
               txContext.getCurrentTransaction().getTransactionId());
    }
    txContext = new TransactionContext(txClient);
    for (TransactionAware txAware : txAwares) {
      if (txAware instanceof WeakReferenceDelegatingTransactionAware) {
        txAware = ((WeakReferenceDelegatingTransactionAware) txAware).get();
        if (txAware == null) {
          continue;
        }
      }
      txContext.addTransactionAware(txAware);
    }
    txContext.start();
  }

  @Override
  public void finish() throws TransactionFailureException {
    // copied from TransactionContext so it behaves exactly the same in this case
    Preconditions.checkState(txContext != null, "Cannot finish tx that has not been started");
    try {
      txContext.finish();
    } finally {
      txContext = null;
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
      txContext = null;
    }
  }
}
