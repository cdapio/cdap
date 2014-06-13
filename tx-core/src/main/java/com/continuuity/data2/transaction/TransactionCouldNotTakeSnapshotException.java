package com.continuuity.data2.transaction;

/**
 * Throw when taking a snapshot fails.
 */
public class TransactionCouldNotTakeSnapshotException extends Exception {
  public TransactionCouldNotTakeSnapshotException(String message) {
    super(message);
  }
}
