package com.continuuity.data.operation.executor.omid;

import java.util.Collections;
import java.util.List;

/**
 *
 */
public class TransactionResult {

  private List<Undo> undos;
  private boolean success;

  /**
   * Constructur for failure case, passing in the operations to be undone
   * @param undos the operations to be undone for the rollback
   */
  public TransactionResult(List<Undo> undos) {
    this.undos = undos;
    this.success = false;
  }

  /**
   * Constructor for success case, there is nothing to be undone
   */
  public TransactionResult() {
    this.undos = Collections.EMPTY_LIST;
    this.success = true;
  }

  public List<Undo> getUndos() {
    return undos;
  }

  public boolean isSuccess() {
    return success;
  }
}