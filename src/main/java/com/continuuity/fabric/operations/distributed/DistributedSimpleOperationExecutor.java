package com.continuuity.fabric.operations.distributed;

import com.continuuity.fabric.engine.hbase.HBaseSimpleExecutor;
import com.continuuity.fabric.operations.SimpleOperationExecutor;
import com.continuuity.fabric.operations.SyncReadTimeoutException;
import com.continuuity.fabric.operations.WriteOperation;
import com.continuuity.fabric.operations.impl.OrderedRead;
import com.continuuity.fabric.operations.impl.QueuePop;
import com.continuuity.fabric.operations.impl.Read;
import com.continuuity.fabric.operations.impl.ReadModifyWrite;
import com.continuuity.fabric.operations.impl.Write;

/**
 * Performs operations in the simplest way possible on top of the distributed
 * data fabric.
 *
 * This executor utilizes the {@link HBaseSimpleExecutor}.
 */
public class DistributedSimpleOperationExecutor
implements SimpleOperationExecutor {

  HBaseSimpleExecutor executor;

  public DistributedSimpleOperationExecutor(HBaseSimpleExecutor executor) {
    this.executor = executor;
  }

  @Override
  public boolean execute(WriteOperation[] writes) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public byte[] execute(Read read) throws SyncReadTimeoutException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String execute(QueuePop pop) throws SyncReadTimeoutException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String execute(OrderedRead orderedRead) throws SyncReadTimeoutException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean execute(Write write) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(ReadModifyWrite rmw) {
    // TODO Auto-generated method stub
    return false;
  }

}
