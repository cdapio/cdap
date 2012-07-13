package com.continuuity.data.operation.executor;

public class BatchOperationException extends Exception {
  private static final long serialVersionUID = -3098934815016999521L;

  public BatchOperationException(String msg) {
    super(msg);
  }

  public BatchOperationException(String msg, Exception cause) {
    super(msg, cause);
  }
}
