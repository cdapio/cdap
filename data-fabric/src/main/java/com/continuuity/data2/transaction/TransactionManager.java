package com.continuuity.data2.transaction;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.StatusCode;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * todo: merge with TransactionExecutor class?
 */
public class TransactionManager {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);

  private final Iterable<TransactionAware> txAware;
  private final TransactionSystemClient txSystemClient;

  // TransactionAgent can be associated only with one transaction at a time.
  private Transaction currentTx;

  public TransactionManager(TransactionSystemClient txSystemClient, Iterable<TransactionAware> txAware) {
    this.txSystemClient = txSystemClient;
    this.txAware = txAware;
  }

  public void start() throws OperationException {
    currentTx = txSystemClient.startShort();
    propagateToTxAwares(currentTx);
  }

  public void abort() throws OperationException {
    abortTxAwareDataSets();
  }

  public void finish() throws OperationException {
    commitTxAwareDataSets();
    postCommitTxAwareDataSets();
    currentTx = null;
  }

  // sets tx to be used by txAware datasets
  private void propagateToTxAwares(Transaction currentTx) {
    // currentTx may not be set up before that (e.g. in detached tx agent case)
    this.currentTx = currentTx;
    for (TransactionAware txnl : txAware) {
      txnl.startTx(currentTx);
    }
  }

  private void postCommitTxAwareDataSets() throws OperationException {
    OperationException error = null;
    for (TransactionAware txAware : this.txAware) {
      try {
        txAware.postTxCommit();
      } catch (Exception e) {
        LOG.error("failed to post commmit transaction " + currentTx.getWritePointer(), e);
        // NOTE: this does not cause roll back since the transaction is already committed.
        error = new OperationException(StatusCode.INVALID_TRANSACTION,
                                       "failed to post commit tx" + currentTx.getWritePointer(), e);
      }
    }
    if (error != null) {
      throw error;
    }
  }

  private void commitTxAwareDataSets() throws OperationException {

    // 1. figure out whether the transaction can commit
    List<byte[]> changes = Lists.newArrayList();
    for (TransactionAware txnl : txAware) {
      changes.addAll(txnl.getTxChanges());
    }
    if (changes.size() > 0) {
      if (!txSystemClient.canCommit(currentTx, changes)) {
        // the app-fabric runtime will call abort() after that, so no need to do extra steps here
        throw new OperationException(StatusCode.TRANSACTION_CONFLICT, "Cannot commit tx: conflict detected");
      }
    }

    // 2. flush all data sets in the tx
    flushTxAwareDataSets();

    // 3. commit the transaction (this can still fail)
    if (!txSystemClient.commit(currentTx)) {
      // the app-fabric runtime will call abort() after that, so no need to do extra steps to undo the flush
      throw new OperationException(StatusCode.INVALID_TRANSACTION, "failed to commit tx (2nd phase)");
    }
  }

  private void flushTxAwareDataSets() throws OperationException {
    for (TransactionAware txAware : this.txAware) {
      boolean success;
      try {
        success = txAware.commitTx();
      } catch (Exception e) {
        throw new OperationException(StatusCode.INVALID_TRANSACTION, "failed to flush tx", e);
      }
      if (!success) {
        throw new OperationException(StatusCode.INVALID_TRANSACTION,
                                     String.format("failed to flush tx for %s", txAware.getClass()));
      }
    }
  }

  private void abortTxAwareDataSets() throws OperationException {
    boolean aborted = true;
    OperationException error = null;
    for (TransactionAware txAware : this.txAware) {
      try {
        aborted = txAware.rollbackTx() && aborted;
      } catch (Exception e) {
        LOG.error("failed to abort transaction " + currentTx.getWritePointer(), e);
        // NOTE: we keep rolling back here even though we know that abort is already failed. The more is rolled back
        //       the less garbage we have in the system
        error = new OperationException(StatusCode.INVALID_TRANSACTION,
                                       "failed to abort tx" + currentTx.getWritePointer(), e);
      }
    }

    if (error != null) {
      throw error;
    }

    if (!aborted) {
      throw  new OperationException(StatusCode.INVALID_TRANSACTION,
                                    "failed to abort tx" + currentTx.getWritePointer());
    }

    // it can be null to allow doing abort multiple times
    if (currentTx != null) {
      txSystemClient.abort(currentTx);
    }
    currentTx = null;
  }

}
