package com.continuuity.fabric.operations.memory;

import com.continuuity.fabric.engine.memory.MemorySimpleExecutor;
import com.continuuity.fabric.operations.SimpleOperationExecutor;
import com.continuuity.fabric.operations.SyncReadTimeoutException;
import com.continuuity.fabric.operations.WriteOperation;
import com.continuuity.fabric.operations.impl.OrderedRead;
import com.continuuity.fabric.operations.impl.QueuePop;
import com.continuuity.fabric.operations.impl.Read;
import com.continuuity.fabric.operations.impl.ReadModifyWrite;
import com.continuuity.fabric.operations.impl.Write;

public class MemorySimpleOperationExecutor implements SimpleOperationExecutor {

  private final MemorySimpleExecutor executor;

  public MemorySimpleOperationExecutor(MemorySimpleExecutor executor) {
    this.executor = executor;
  }

  @Override
  public boolean execute(Write write) {
    this.executor.write(write.getKey(), write.getValue());
    return true;
  }

  @Override
  public boolean execute(ReadModifyWrite rmw) {
    this.executor.readModifyWrite(rmw.getKey(), rmw.getModifier());
    return true;
  }

  @Override
  public boolean execute(WriteOperation[] writes) {
    for (WriteOperation write : writes) {
      // bah!
      if (write instanceof Write) {
        if (!execute((Write)write)) return false;
      } else if (write instanceof ReadModifyWrite) {
        if (!execute((ReadModifyWrite)write)) return false;
      }
    }
    return true;
  }

  @Override
  public byte [] execute(Read read) throws SyncReadTimeoutException {
    byte [] key = read.getKey();
    return this.executor.read(key);
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

  
}
