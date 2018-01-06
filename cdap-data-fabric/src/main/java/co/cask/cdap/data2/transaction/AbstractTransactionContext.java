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

package co.cask.cdap.data2.transaction;

import co.cask.cdap.api.common.Bytes;
import com.google.common.base.Preconditions;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;

import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nullable;

/**
 * An abstract implementation of {@link TransactionContext} for governing general transaction lifecycle.
 * This class provides flexibility on the set of
 * {@link TransactionAware} to be participated in transactions.
 *
 * This abstract class extends from {@link TransactionContext} just for inheriting the type and public methods
 * signatures. It is not using any functionality from the parent class.
 */
public abstract class AbstractTransactionContext extends TransactionContext {

  private final TransactionSystemClient txClient;
  private Transaction currentTx;

  protected AbstractTransactionContext(TransactionSystemClient txClient) {
    // Passing null to parent to make sure nothing in parent class would work
    super(null);
    this.txClient = txClient;
  }

  /**
   * Provides the list of {@link TransactionAware} that are participating in transaction.
   */
  protected abstract Iterable<TransactionAware> getTransactionAwares();

  /**
   * Adds the given {@link TransactionAware} to participating in transaction.
   *
   * @param txAware the {@link TransactionAware} to add
   * @return {@code true} if the given {@link TransactionAware} needs to participate in current transaction,
   *                      {@code false} otherwise
   */
  protected abstract boolean doAddTransactionAware(TransactionAware txAware);

  /**
   * Removes the given {@link TransactionAware} from participating in transaction.
   *
   * @param txAware the {@link TransactionAware} to remove
   * @return {@code true} if removed, {@code false} otherwise
   */
  protected abstract boolean doRemoveTransactionAware(TransactionAware txAware);

  /**
   * Performs cleanup task after a transaction is completed (either committed or aborted).
   */
  protected void cleanup() {
    // No-op
  }

  @Override
  public final boolean addTransactionAware(TransactionAware txAware) {
    boolean added = doAddTransactionAware(txAware);

    // If there is an active transaction, call startTx on the given txAware as well
    if (added && currentTx != null) {
      txAware.startTx(currentTx);
    }
    return added;
  }

  @Override
  public final boolean removeTransactionAware(TransactionAware txAware) {
    Preconditions.checkState(currentTx == null, "Cannot remove TransactionAware while there is an active transaction.");
    return doRemoveTransactionAware(txAware);
  }

  @Override
  public void start() throws TransactionFailureException {
    Preconditions.checkState(currentTx == null, "Already have an active transaction.");
    currentTx = txClient.startShort();
    startAllTxAwares();
  }

  @Override
  public void start(int timeout) throws TransactionFailureException {
    Preconditions.checkState(currentTx == null, "Already have an active transaction.");
    currentTx = txClient.startShort(timeout);
    startAllTxAwares();
  }

  @Override
  public void finish() throws TransactionFailureException {
    Preconditions.checkState(currentTx != null, "Cannot finish tx that has not been started");
    // each of these steps will abort and rollback the tx in case if errors, and throw an exception
    checkForConflicts();
    persist();
    commit();

    try {
      postCommit();
    } finally {
      currentTx = null;
      cleanup();
    }
  }

  @Override
  public void abort() throws TransactionFailureException {
    abort(null);
  }

  @Override
  public void abort(@Nullable TransactionFailureException cause) throws TransactionFailureException {
    if (currentTx == null) {
      // might be called by some generic exception handler even though already aborted/finished - we allow that
      return;
    }
    try {
      boolean success = true;
      for (TransactionAware txAware : getTransactionAwares()) {
        try {
          success = txAware.rollbackTx() && success;
        } catch (Throwable t) {
          TransactionFailureException tfe = createTransactionFailure("roll back changes in", txAware, t);
          if (cause == null) {
            cause = tfe;
          } else {
            cause.addSuppressed(tfe);
          }
          success = false;
        }
      }
      try {
        if (success) {
          txClient.abort(currentTx);
        } else {
          txClient.invalidate(currentTx.getTransactionId());
        }
      } catch (Throwable t) {
        if (cause == null) {
          cause = new TransactionFailureException(
            String.format("Error while calling transaction service to %s transaction %d.",
                          success ? "abort" : "invalidate", currentTx.getTransactionId()));
        } else {
          cause.addSuppressed(t);
        }
      }
      if (cause != null) {
        throw cause;
      }
    } finally {
      currentTx = null;
      cleanup();
    }
  }

  @Override
  public void checkpoint() throws TransactionFailureException {
    Preconditions.checkState(currentTx != null, "Cannot checkpoint tx that has not been started");
    persist();
    try {
      currentTx = txClient.checkpoint(currentTx);
      // update the current transaction with all TransactionAwares
      for (TransactionAware txAware : getTransactionAwares()) {
        txAware.updateTx(currentTx);
      }
    } catch (TransactionFailureException e) {
      abort(e);
    } catch (Throwable e) {
      abort(new TransactionFailureException(
        String.format("Exception from checkpoint for transaction %d.", currentTx.getTransactionId()), e));
    }
  }

  @Nullable
  @Override
  public Transaction getCurrentTransaction() {
    return currentTx;
  }

  /**
   * Calls {@link TransactionAware#startTx(Transaction)} on all {@link TransactionAware}.
   */
  private void startAllTxAwares() throws TransactionFailureException {
    for (TransactionAware txAware : getTransactionAwares()) {
      try {
        txAware.startTx(currentTx);
      } catch (Throwable t) {
        try {
          txClient.abort(currentTx);
          throw createTransactionFailure("start", txAware, t);
        } finally {
          currentTx = null;
        }
      }
    }
  }

  /**
   * Collects the set of changes across all {@link TransactionAware}s by calling {@link TransactionAware#getTxChanges()}
   * and checks if conflicts will arise when the transaction is going to be committed.
   */
  private void checkForConflicts() throws TransactionFailureException {
    Set<byte[]> changes = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    for (TransactionAware txAware : getTransactionAwares()) {
      try {
        changes.addAll(txAware.getTxChanges());
      } catch (Throwable t) {
        abort(createTransactionFailure("retrieve changes from", txAware, t));
      }
    }

    // If there is no changes, no need to call canCommit
    if (changes.isEmpty()) {
      return;
    }

    try {
      txClient.canCommitOrThrow(currentTx, changes);
    } catch (TransactionFailureException e) {
      abort(e);
    } catch (Throwable e) {
      abort(new TransactionFailureException(
        String.format("Exception from canCommit for transaction %d.", currentTx.getTransactionId()), e));
    }
  }

  /**
   * Calls {@link TransactionAware#commitTx()} on all {@link TransactionAware} to persist pending changes.
   */
  private void persist() throws TransactionFailureException {
    for (TransactionAware txAware : getTransactionAwares()) {
      boolean success = false;
      Throwable cause = null;
      try {
        success = txAware.commitTx();
      } catch (Throwable e) {
        cause = e;
      }
      if (!success) {
        abort(createTransactionFailure("persist changes of", txAware, cause));
      }
    }
  }

  /**
   * Commits the current transaction.
   */
  private void commit() throws TransactionFailureException {
    try {
      txClient.commitOrThrow(currentTx);
    } catch (TransactionFailureException e) {
      abort(e);
    } catch (Throwable e) {
      abort(new TransactionFailureException(
        String.format("Exception from commit for transaction %d.", currentTx.getTransactionId()), e));
    }
  }

  /**
   * Calls the {@link TransactionAware#postTxCommit()} on all {@link TransactionAware}.
   */
  private void postCommit() throws TransactionFailureException {
    TransactionFailureException cause = null;
    for (TransactionAware txAware : getTransactionAwares()) {
      try {
        txAware.postTxCommit();
      } catch (Throwable t) {
        TransactionFailureException tfe = createTransactionFailure("perform post-commit for", txAware, t);
        if (cause == null) {
          cause = tfe;
        } else {
          cause.addSuppressed(tfe);
        }
      }
    }
    if (cause != null) {
      throw cause;
    }
  }

  protected TransactionFailureException createTransactionFailure(String action,
                                                                 TransactionAware txAware,
                                                                 Throwable cause) {
    String txAwareName;
    Throwable thrownForName = null;
    try {
      txAwareName = txAware.getTransactionAwareName();
    } catch (Throwable t) {
      thrownForName = t;
      txAwareName = "unknown";
    }
    TransactionFailureException tfe = new TransactionFailureException(
      String.format("Unable to %s transaction-aware '%s' for transaction %d",
                    action, txAwareName, currentTx.getTransactionId()), cause);
    if (thrownForName != null) {
      tfe.addSuppressed(thrownForName);
    }
    return tfe;
  }
}
