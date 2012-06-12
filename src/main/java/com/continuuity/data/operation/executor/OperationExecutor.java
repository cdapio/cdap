package com.continuuity.data.operation.executor;


import com.continuuity.api.data.ReadOperationExecutor;
import com.continuuity.data.table.OVCTableHandle;

/**
 * TODO: Write some docs
 *
 * Executes read and write operations.
 *
 * Writes return true or false as to whether they succeeded or not.
 */
public interface OperationExecutor
  extends ReadOperationExecutor, WriteOperationExecutor, BatchOperationExecutor,
          InternalReadOperationExecutor {

  public OVCTableHandle getTableHandle();

}
