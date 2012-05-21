package com.continuuity.data.operation.executor.omid;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.SyncReadTimeoutException;
import com.continuuity.data.engine.ReadPointer;
import com.continuuity.data.engine.VersionedQueueTable;
import com.continuuity.data.engine.VersionedTable;
import com.continuuity.data.engine.VersionedTableHandle;
import com.continuuity.data.operation.CompareAndSwap;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.OrderedRead;
import com.continuuity.data.operation.OrderedWrite;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadCounter;
import com.continuuity.data.operation.ReadModifyWrite;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.executor.TransactionalOperationExecutor;
import com.continuuity.data.operation.queue.QueueAck;
import com.continuuity.data.operation.queue.QueueEntry;
import com.continuuity.data.operation.queue.QueuePop;
import com.continuuity.data.operation.queue.QueuePush;
import com.continuuity.data.operation.type.WriteOperation;

/**
 *
 */
public class OmidTransactionalOperationExecutor implements
    TransactionalOperationExecutor {

  final TransactionOracle oracle;

  final VersionedTableHandle tableHandle;

  final VersionedTable randomTable;

  final VersionedTable orderedTable;

  final VersionedQueueTable queueTable;

  public OmidTransactionalOperationExecutor(TransactionOracle oracle,
      VersionedTableHandle tableHandle) {
    this.oracle = oracle;
    this.tableHandle = tableHandle;
    this.randomTable = tableHandle.getTable(Bytes.toBytes("random"));
    this.orderedTable = tableHandle.getTable(Bytes.toBytes("ordered"));
    this.queueTable = tableHandle.getQueueTable(Bytes.toBytes("queues"));
  }

  @Override
  public byte[] execute(Read read) throws SyncReadTimeoutException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long execute(ReadCounter readCounter) throws SyncReadTimeoutException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Map<byte[], byte[]> execute(OrderedRead orderedRead)
      throws SyncReadTimeoutException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueueEntry execute(QueuePop pop) throws SyncReadTimeoutException,
      InterruptedException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean execute(List<WriteOperation> writes)
  throws OmidTransactionException {
    ImmutablePair<ReadPointer,Long> pointer = this.oracle.getNewPointer();
    ReadPointer readPointer = pointer.getFirst();
    long txid = pointer.getSecond();
    
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(QueueAck ack) {
    // only one supported individually (still done in a transaction!)
    return execute(Arrays.asList(new WriteOperation [] { ack }));
  }

  // Single Write Operations (UNSUPPORTED IN TRANSACTIONAL!)

  private void unsupported() {
    throw new RuntimeException(
        "Single write operations are not supported by transactional executors");
  }

  @Override
  public boolean execute(Write write) {
    unsupported();
    return false;
  }
  @Override
  public boolean execute(OrderedWrite write) {
    unsupported();
    return false;
  }

  @Override
  public boolean execute(ReadModifyWrite rmw) {
    unsupported();
    return false;
  }

  @Override
  public boolean execute(Increment inc) {
    unsupported();
    return false;
  }

  @Override
  public boolean execute(CompareAndSwap cas) {
    unsupported();
    return false;
  }

  @Override
  public boolean execute(QueuePush push) {
    unsupported();
    return false;
  }
}
