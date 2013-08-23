package com.continuuity.gateway.v2.txmanager;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Base class for Transaction Managers. This class is not thread-safe, if one thread has started a txn,
 * then no other threads can start a txn using the same manager till the txn is complete.
 */
public abstract class AbstractTxManager {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractTxManager.class);

  private final OperationExecutor opex;

  // TransactionAware objects involved in the ongoing txn
  private Set<TransactionAware> txnTxAwares;
  private Transaction transaction;

  public AbstractTxManager(OperationExecutor opex) {
    this.opex = opex;
  }

  protected abstract Set<TransactionAware> getTransactionAwares();

  public void start() throws OperationException {
    transaction = opex.start();
    txnTxAwares = ImmutableSet.copyOf(getTransactionAwares());
    for (TransactionAware txAware : txnTxAwares) {
      txAware.startTx(transaction);
    }
  }

  public void commit() throws OperationException {
    // Collects change sets
    Set<byte[]> changeSet = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
    for (TransactionAware txAware : txnTxAwares) {
      changeSet.addAll(txAware.getTxChanges());
    }

    // Check for conflicts
    if (!opex.canCommit(transaction, changeSet)) {
      throw new OperationException(StatusCode.TRANSACTION_CONFLICT, "Cannot commit tx: conflict detected");
    }

    // Persist changes
    for (TransactionAware txAware : txnTxAwares) {
      try {
        if (!txAware.commitTx()) {
          throw new OperationException(StatusCode.INVALID_TRANSACTION, "Fails to commit tx.");
        }
      } catch (Exception e) {
        throw new OperationException(StatusCode.INVALID_TRANSACTION, "Fails to commit tx.", e);
      }
    }

    // Make visible
    if (!opex.commit(transaction)) {
      throw new OperationException(StatusCode.INVALID_TRANSACTION, "Fails to make tx visible.");
    }

    // Post commit call
    for (TransactionAware txAware : txnTxAwares) {
      try {
        txAware.postTxCommit();
      } catch (Throwable t) {
        LOG.error("Post commit call failure.", t);
      }
    }
  }

  public void abort() throws OperationException {
    for (TransactionAware txAware : txnTxAwares) {
      try {
        if (!txAware.rollbackTx()) {
          LOG.error("Fail to rollback: {}", txAware);
        }
      } catch (Exception e) {
        LOG.error("Exception in rollback: {}", txAware, e);
      }
    }
    opex.abort(transaction);
  }
}
