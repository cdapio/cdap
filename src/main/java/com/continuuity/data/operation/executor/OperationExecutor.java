package com.continuuity.data.operation.executor;


/**
 * TODO: Write some docs
 *
 * Executes read and write operations.
 *
 * Writes return true or false as to whether they succeeded or not.
 */
public interface OperationExecutor
  extends ReadOperationExecutor, WriteOperationExecutor, BatchOperationExecutor,
          InternalOperationExecutor {

  public String getName();
}
