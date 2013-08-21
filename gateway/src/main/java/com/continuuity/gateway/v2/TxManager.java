package com.continuuity.gateway.v2;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;

/**
 * Transaction manager to handle transactions.
 */
public class TxManager {
  private static final Logger LOG = LoggerFactory.getLogger(TxManager.class);

  private final OperationExecutor opex;
  private final Collection<TransactionAware> txAwares;
  private Transaction transaction;

  public TxManager(OperationExecutor opex, Iterable<TransactionAware> txAware) {
    this.opex = opex;
    this.txAwares = ImmutableList.copyOf(txAware);
  }

  public void start() throws OperationException {
    transaction = opex.start();
    for (TransactionAware txAware : txAwares) {
      txAware.startTx(transaction);
    }
  }

  public void commit() throws OperationException {
    // Collects change sets
    Set<byte[]> changeSet = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
    for (TransactionAware txAware : txAwares) {
      changeSet.addAll(txAware.getTxChanges());
    }

    // Check for conflicts
    if (!opex.canCommit(transaction, changeSet)) {
      throw new OperationException(StatusCode.TRANSACTION_CONFLICT, "Cannot commit tx: conflict detected");
    }

    // Persist changes
    for (TransactionAware txAware : txAwares) {
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
    for (TransactionAware txAware : txAwares) {
      try {
        txAware.postTxCommit();
      } catch (Throwable t) {
        LOG.error("Post commit call failure.", t);
      }
    }
  }

  public void abort() throws OperationException {
    for (TransactionAware txAware : txAwares) {
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
