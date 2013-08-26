package com.continuuity.data.operation.executor;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides default implementation for some of the TransactionAgent methods.
 */
public abstract class AbstractTransactionAgent implements TransactionAgent {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionAgent.class);

  // this counts successful operations
  private final AtomicInteger succeeded = new AtomicInteger(0);
  // this counts failed operations
  private final AtomicInteger failed = new AtomicInteger(0);

  // the actual operation executor
  protected final OperationExecutor opex;
  // the operation context for all operations
  protected final OperationContext context;

  // NOTE: the tx2 logic is put in TransactionAgent to  minimize destrucion of early integration of txds2.
  //       TransactionAgent will go away as soon as we complete transition
  private final Iterable<TransactionAware> txAware;
  private final TransactionSystemClient txSystemClient;

  // TransactionAgent can be associated only with one transaction at a time.
  private Transaction currentTx;

  /**
   * Constructor must pass the operation executor and context.
   * @param opex the actual operation executor
   * @param context the operation context for all operations
   */
  public AbstractTransactionAgent(OperationExecutor opex, OperationContext context,
                                  Iterable<TransactionAware> txAware, TransactionSystemClient txSystemClient) {
    this.opex = opex;
    this.context = context;
    this.txAware = txAware;
    this.txSystemClient = txSystemClient;
  }

  @Override
  public void start() throws OperationException {
    succeeded.set(0);
    failed.set(0);
    currentTx = txSystemClient.start();
    propagateToTxAwares(currentTx);
  }

  // sets tx to be used by txAware datasets
  protected void propagateToTxAwares(com.continuuity.data2.transaction.Transaction currentTx) {
    for (TransactionAware txnl : txAware) {
      txnl.startTx(currentTx);
    }
  }

  @Override
  public void abort() throws OperationException {
    abortTxAwareDataSets();
  }

  @Override
  public void finish() throws OperationException {
    commitTxAwareDataSets();
    postCommitTxAwareDataSets();
    currentTx = null;
  }

  @Override
  public void flush() throws OperationException {
    commitTxAwareDataSets();
  }

  @Override
  public int getSucceededCount() {
    return succeeded.get();
  }

  @Override
  public int getFailedCount() {
    return failed.get();
  }

  @Override
  public Transaction getCurrentTx() {
    return currentTx;
  }

  /**
   * Add 1 to the number of succeeded operations.
   */
  protected void succeededOne() {
    succeeded.incrementAndGet();
  }

  /**
   * Add a delta to the number of succeeded operations.
   * @param count how many operations succeeded
   */
  protected void succeededSome(int count) {
    succeeded.addAndGet(count);
  }

  /**
   * Add 1 to the number of failed operations.
   */
  protected void failedOne() {
    failed.incrementAndGet();
  }

  /**
   * Add a delta to the number of failed operations.
   * @param count how many operations failed
   */
  protected void failedSome(int count) {
    failed.addAndGet(count);
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

    for (TransactionAware txAware : this.txAware) {
      try {
        if (!txAware.commitTx()) {
          // the app-fabric runtime will call abort() after that, so no need to do extra steps here
          throw new OperationException(StatusCode.INVALID_TRANSACTION,
                                       String.format("failed to commit tx for %s", txAware.getClass()));
        }
      } catch (Exception e) {
        // the app-fabric runtime will call abort() after that, so no need to do extra steps here
        throw new OperationException(StatusCode.INVALID_TRANSACTION, "failed to commit tx", e);
      }
    }

    // ANDREAS: isn't there a race condition here? canCommit() returns true, then we commit all datasets,
    // then we attempt to commit() which detects a conflict. Now the dataset changes will not be rolled back.
    // chances for this are high: until we call canCommit(), all changes happen in memory and are fast. The
    // committing/flushing of all datasets is the slow part... so conflicts most likely happen now.

    // TERENCE: The commit call will check for conflicts again in the TX oracle, hence the call to commit
    // could fail if there are new conflicts introduced between canCommit and commit call, causing exception
    // to be thrown, hence triggering rollback from the app-fabric.
    if (!txSystemClient.commit(currentTx)) {
      // the app-fabric runtime will call abort() after that, so no need to do extra steps here
      throw new OperationException(StatusCode.INVALID_TRANSACTION, "failed to commit tx (2nd phase)");
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
