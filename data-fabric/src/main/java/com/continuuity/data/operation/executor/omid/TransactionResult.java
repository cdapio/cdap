package com.continuuity.data.operation.executor.omid;

import com.continuuity.data.operation.ttqueue.QueueFinalize;

import java.util.List;

/**
 * The result of committing a transaction. Can represent failure along with the operations to be undone, or success.
 */
public class TransactionResult {

  private List<Undo> undos;
  private QueueFinalize finalize;
  private boolean success;

  /**
   * Constructur for failure case, passing in the operations to be undone.
   * @param undos the operations to be undone for the rollback
   */
  public TransactionResult(List<Undo> undos) {
    this.undos = undos;
    this.finalize = null;
    this.success = false;
  }

  /**
   * Constructor for success case. There is nothing to be undone, but we
   * return the undos anyway, as the opex relies on this for metrics. We
   * also return the (optional) queue finalize operation that has to be
   * done by opex after successful commit.
   */
  public TransactionResult(List<Undo> undos, QueueFinalize finalize) {
    this.undos = undos;
    this.finalize = finalize;
    this.success = true;
  }

  public List<Undo> getUndos() {
    return undos;
  }

  public boolean isSuccess() {
    return success;
  }

  public QueueFinalize getFinalize() {
    return finalize;
  }
}
