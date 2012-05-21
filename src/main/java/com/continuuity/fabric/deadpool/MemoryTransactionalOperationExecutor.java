package com.continuuity.fabric.deadpool;

import java.util.List;
import java.util.Map;

import com.continuuity.data.SyncReadTimeoutException;
import com.continuuity.data.access.DelegationToken;
import com.continuuity.data.operation.CompareAndSwap;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.OrderedRead;
import com.continuuity.data.operation.OrderedWrite;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadModifyWrite;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.executor.TransactionalOperationExecutor;
import com.continuuity.data.operation.executor.omid.RowTracker;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.queue.QueueEntry;
import com.continuuity.data.operation.queue.QueuePop;
import com.continuuity.data.operation.queue.QueuePush;
import com.continuuity.data.operation.type.WriteOperation;

public abstract class MemoryTransactionalOperationExecutor
implements TransactionalOperationExecutor {

  private final NativeTransactionalExecutor executor;

  private final TransactionOracle oracle;
  
  private final RowTracker rowTracker;

  public MemoryTransactionalOperationExecutor(
      NativeTransactionalExecutor executor,
      TransactionOracle oracle,
      RowTracker rowTracker) {
    this.executor = executor;
    this.oracle = oracle;
    this.rowTracker = rowTracker;
  }

  @Override
  public boolean execute(List<WriteOperation> writes) {
    // Open the transaction
    long txid = oracle.getWriteTxid();
    // Write them in order, for now
    for (WriteOperation write : writes) {
      DelegationToken token;
    }
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(Write write) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public byte[] execute(Read read) throws SyncReadTimeoutException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueueEntry execute(QueuePop pop) throws SyncReadTimeoutException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<byte[], byte[]> execute(OrderedRead orderedRead) throws SyncReadTimeoutException {
    // TODO Auto-generated method stub
    return null;
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
