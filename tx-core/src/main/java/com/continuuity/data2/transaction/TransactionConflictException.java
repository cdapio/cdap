package com.continuuity.data2.transaction;

/**
 *
 */
public class TransactionConflictException extends TransactionFailureException {
  public TransactionConflictException(String message) {
    super(message);
  }

  public TransactionConflictException(String message, Throwable cause) {
    super(message, cause);
  }
}
