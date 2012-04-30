package com.continuuity.fabric.operations.memory;

import com.continuuity.fabric.engine.memory.MemoryTransactionalExecutor;
import com.continuuity.fabric.operations.SyncReadTimeoutException;
import com.continuuity.fabric.operations.TransactionalOperationExecutor;
import com.continuuity.fabric.operations.WriteOperation;
import com.continuuity.fabric.operations.impl.OrderedRead;
import com.continuuity.fabric.operations.impl.QueuePop;
import com.continuuity.fabric.operations.impl.Read;
import com.continuuity.fabric.operations.impl.ReadModifyWrite;
import com.continuuity.fabric.operations.impl.Write;

public class MemoryTransactionalOperationExecutor
implements TransactionalOperationExecutor {

  @SuppressWarnings("unused")
  private final MemoryTransactionalExecutor executor;

  public MemoryTransactionalOperationExecutor(
      MemoryTransactionalExecutor executor) {
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
