package com.continuuity.persistence.ops.execs;

/**
 * Exception occurred performing {@link Operation}s.
 */
public class OperationExecutionException extends Exception {
  private static final long serialVersionUID = 8366852592027585136L;
  public OperationExecutionException(String msg) {
    super(msg);
  }
}
