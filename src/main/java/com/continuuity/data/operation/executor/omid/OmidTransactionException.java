package com.continuuity.data.operation.executor.omid;

import com.continuuity.data.operation.executor.TransactionException;

public class OmidTransactionException extends TransactionException {
  private static final long serialVersionUID = 7253274239927817754L;

  public OmidTransactionException(int status, String msg) {
    super(status, msg);
  }
}
