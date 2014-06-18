package com.continuuity.data2.transaction;

/**
 * Thrown when transaction has timed out.
 */
public class TransactionNotInProgressException extends Exception {
  public TransactionNotInProgressException(String message) {
    super(message);
  }
}
