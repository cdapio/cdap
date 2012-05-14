package com.continuuity.fabric.operations.distributed;

import java.util.List;
import java.util.Map;

import com.continuuity.fabric.engine.NativeExecutor;
import com.continuuity.fabric.engine.NativeOperation;
import com.continuuity.fabric.operations.Operation;
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

/**
 * Executes a list of {@link Operation}s by converting them into a list of
 * {@link NativeOperation}s and then executing them with a {@link NativeExecutor}.
 */
public abstract class DistributedTransactionalOperationExecutor
implements TransactionalOperationExecutor {

  @Override
  public boolean execute(List<WriteOperation> writes) {
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
