package com.continuuity.data.operation.executor;

import com.continuuity.api.data.OperationException;

/**
 * Exception thrown when a transaction fails.
 */
public class TransactionException extends OperationException {
  private static final long serialVersionUID = 6326789949414855631L;

  public TransactionException(int statusCode, String msg) {
    super(statusCode, msg);
  }
}
