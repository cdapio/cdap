package com.continuuity.data2.transaction;

/**
 *
 */
public class TransactionFailureException extends Exception {
  public TransactionFailureException(String message) {
    super(message);
  }

  public TransactionFailureException(String message, Throwable cause) {
    super(message, cause);
  }
}
