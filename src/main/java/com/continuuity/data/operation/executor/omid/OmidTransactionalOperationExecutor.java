/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.operation.executor.omid;

import java.util.ArrayList;
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
import com.continuuity.data.operation.Delete;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.OperationGenerator;
import com.continuuity.data.operation.OrderedRead;
import com.continuuity.data.operation.OrderedWrite;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadCounter;
import com.continuuity.data.operation.ReadModifyWrite;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.executor.TransactionalOperationExecutor;
import com.continuuity.data.operation.executor.omid.memory.MemoryRowSet;
import com.continuuity.data.operation.queue.QueueAck;
import com.continuuity.data.operation.queue.QueueEntry;
import com.continuuity.data.operation.queue.QueuePop;
import com.continuuity.data.operation.queue.QueuePush;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAdmin.GetGroupID;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueueInvalidate;
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

  static final byte [] COLUMN = Bytes.toBytes("c");

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
    return read(read, this.oracle.getReadPointer());
  }

  byte [] read(Read read, ReadPointer pointer) {
    return this.randomTable.get(read.getKey(), COLUMN, pointer);
  }
  
  @Override
  public long execute(ReadCounter readCounter) throws SyncReadTimeoutException {
    byte [] value = this.randomTable.get(readCounter.getKey(), COLUMN,
        this.oracle.getReadPointer());
    if (value == null || value.length != 8) return 0;
    return Bytes.toLong(value);
  }

  @Override
  public boolean execute(List<WriteOperation> writes)
  throws OmidTransactionException {
    // Open transaction
    ImmutablePair<ReadPointer,Long> pointer = startTransaction();
    
    // Execute operations (in order for now)
    RowSet rows = new MemoryRowSet();
    List<Delete> deletes = new ArrayList<Delete>(writes.size());
    for (WriteOperation write : writes) {
      WriteTransactionResult writeTxReturn = dispatchWrite(write, pointer);
      if (!writeTxReturn.success) {
        // Write operation failed
        abortTransaction(pointer, deletes);
        return false;
      } else {
        // Write was successful.  Store delete if we need to abort and continue
        deletes.addAll(writeTxReturn.deletes);
        rows.addRow(write.getKey());
      }
    }
    
    // All operations completed successfully, commit transaction
    if (!commitTransaction(pointer, rows)) {
      // Commit failed, abort
      abortTransaction(pointer, deletes);
      return false;
    }
    
    // Transaction was successfully committed
    return true;
  }

  private class WriteTransactionResult {
    final boolean success;
    final List<Delete> deletes;
    WriteTransactionResult(boolean success) {
      this(success, new ArrayList<Delete>());
    }
    WriteTransactionResult(boolean success, Delete delete) {
      this(success, Arrays.asList(new Delete [] { delete } ));
    }
    WriteTransactionResult(boolean success, List<Delete> deletes) {
      this.success = success;
      this.deletes = deletes;
    }
  }
  /**
   * Actually perform the various write operations.
   * @param write
   * @param pointer
   * @return
   */
  WriteTransactionResult dispatchWrite(
      WriteOperation write, ImmutablePair<ReadPointer,Long> pointer) {
    if (write instanceof Write) {
      return write((Write)write, pointer);
    } else if (write instanceof ReadModifyWrite) {
      return write((ReadModifyWrite)write, pointer);
    } else if (write instanceof Increment) {
      return write((Increment)write, pointer);
    } else if (write instanceof CompareAndSwap) {
      return write((CompareAndSwap)write, pointer);
    }
    return new WriteTransactionResult(false);
  }
  
  WriteTransactionResult write(Write write,
      ImmutablePair<ReadPointer,Long> pointer) {
    this.randomTable.put(write.getKey(), COLUMN, pointer.getSecond(),
        write.getValue());
    return new WriteTransactionResult(true, new Delete(write.getKey(), COLUMN));
  }
  
  WriteTransactionResult write(ReadModifyWrite write,
      ImmutablePair<ReadPointer,Long> pointer) {
    // read
    byte [] value = this.randomTable.get(write.getKey(), COLUMN,
        pointer.getFirst());
    // modify
    byte [] newValue = write.getModifier().modify(value);
    // write
    this.randomTable.put(write.getKey(), COLUMN, pointer.getSecond(), newValue);
    return new WriteTransactionResult(true, new Delete(write.getKey(), COLUMN));
  }
  
  WriteTransactionResult write(Increment increment,
      ImmutablePair<ReadPointer,Long> pointer) {
    long incremented = this.randomTable.increment(increment.getKey(), COLUMN,
        increment.getAmount(), pointer.getFirst(), pointer.getSecond());
    List<Delete> deletes = new ArrayList<Delete>(2);
    deletes.add(new Delete(increment.getKey(), COLUMN));
    OperationGenerator<Long> generator =
        increment.getPostIncrementOperationGenerator();
    if (generator != null) {
      WriteOperation writeOperation =
          generator.generateWriteOperation(incremented);
      if (writeOperation != null) {
        WriteTransactionResult result = dispatchWrite(writeOperation, pointer);
        deletes.addAll(result.deletes);
        return new WriteTransactionResult(result.success, deletes);
      }
    }
    return new WriteTransactionResult(true, deletes);
  }
  
  WriteTransactionResult write(CompareAndSwap write,
      ImmutablePair<ReadPointer,Long> pointer) {
    boolean casReturn = this.randomTable.compareAndSwap(write.getKey(), COLUMN,
        write.getExpectedValue(), write.getNewValue(), pointer.getFirst(),
        pointer.getSecond());
    return new WriteTransactionResult(casReturn,
        new Delete(write.getKey(), COLUMN));
  }
  
  ImmutablePair<ReadPointer, Long> startTransaction() {
    return this.oracle.getNewPointer();
  }

  boolean commitTransaction(ImmutablePair<ReadPointer, Long> pointer,
      RowSet rows) throws OmidTransactionException {
    return this.oracle.commit(pointer.getSecond(), rows);
  }

  private void abortTransaction(ImmutablePair<ReadPointer,Long> pointer,
      List<Delete> deletes) throws OmidTransactionException {
    // Perform deletes
    for (Delete delete : deletes) {
      assert(delete != null);
      this.randomTable.delete(delete.getKey(), delete.getColumn(),
          pointer.getSecond());
    }
    // Notify oracle
    this.oracle.aborted(pointer.getSecond());
  }
  
  // Queue operations also not supported right now

  @Override
  public boolean execute(QueueAck ack) {
    unsupported("Queue operations not currently supported");
    // NOT SUPPORTED!
    try {
      return execute(Arrays.asList(new WriteOperation [] { ack }));
    } catch (OmidTransactionException e) {
      return false;
    }
  }

  @Override
  public Map<byte[], byte[]> execute(OrderedRead orderedRead)
      throws SyncReadTimeoutException {
    unsupported("Ordered operations not currently supported");
    return null;
  }

  @Override
  public QueueEntry execute(QueuePop pop) throws SyncReadTimeoutException,
      InterruptedException {
    unsupported("Queue operations not currently supported");
    return null;
  }
  // Single Write Operations (UNSUPPORTED IN TRANSACTIONAL!)

  private void unsupported() {
    unsupported(
        "Single write operations are not supported by transactional executors");
  }

  private void unsupported(String msg) {
    throw new RuntimeException(msg);
  }

  @Override
  public boolean execute(Write write) {
    unsupported();
    return false;
  }

  @Override
  public boolean execute(Delete delete) {
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

  @Override
  public DequeueResult execute(QueueDequeue dequeue)
      throws SyncReadTimeoutException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long execute(GetGroupID getGroupId) throws SyncReadTimeoutException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean execute(com.continuuity.data.operation.ttqueue.QueueAck ack) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(QueueEnqueue enqueue) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(QueueInvalidate invalidate) {
    // TODO Auto-generated method stub
    return false;
  }
}
