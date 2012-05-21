package com.continuuity.data.operation.executor;

public class TransactionException extends BatchOperationException {
  private static final long serialVersionUID = 6326789949414855631L;

  public TransactionException(String msg) {
    super(msg);
  }
}
