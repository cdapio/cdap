package com.continuuity.data.operation.executor;

import com.google.common.base.Objects;


/**
 * Result of a {@link BatchOperationExecutor#execute(java.util.List)}.
 */
public class BatchOperationResult {

  private final boolean success;
  private final String msg;

  public BatchOperationResult(final boolean success) {
    this(success, "");
  }

  public BatchOperationResult(boolean success, String msg) {
    this.success = success;
    this.msg = msg;
  }

  public String getMessage() {
    return this.msg;
  }
  
  public boolean isSuccess() {
    return this.success;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("success", this.success)
      .add("msg", this.msg)
      .toString();
  }
}
