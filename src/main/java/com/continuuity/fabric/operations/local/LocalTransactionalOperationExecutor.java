package com.continuuity.fabric.operations.local;

import java.util.Map;

import com.continuuity.fabric.operations.SyncReadTimeoutException;
import com.continuuity.fabric.operations.TransactionalOperationExecutor;
import com.continuuity.fabric.operations.WriteOperation;
import com.continuuity.fabric.operations.impl.CompareAndSwap;
import com.continuuity.fabric.operations.impl.Increment;
import com.continuuity.fabric.operations.impl.OrderedRead;
import com.continuuity.fabric.operations.impl.OrderedWrite;
import com.continuuity.fabric.operations.impl.QueuePop;
import com.continuuity.fabric.operations.impl.QueuePush;
import com.continuuity.fabric.operations.impl.Read;
import com.continuuity.fabric.operations.impl.ReadModifyWrite;
import com.continuuity.fabric.operations.impl.Write;

public class LocalTransactionalOperationExecutor
implements TransactionalOperationExecutor {

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
  public byte[] execute(QueuePop pop) throws SyncReadTimeoutException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<byte[], byte[]> execute(OrderedRead orderedRead) throws SyncReadTimeoutException {
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

  @Override
  public boolean execute(OrderedWrite write) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(QueuePush push) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(Increment inc) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(CompareAndSwap cas) {
    // TODO Auto-generated method stub
    return false;
  }

}
