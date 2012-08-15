/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.operation.executor.omid;

import com.continuuity.api.data.*;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.Undelete;
import com.continuuity.data.operation.WriteOperationComparator;
import com.continuuity.data.operation.executor.BatchOperationResult;
import com.continuuity.data.operation.executor.TransactionalOperationExecutor;
import com.continuuity.data.operation.executor.omid.QueueInvalidate.QueueFinalize;
import com.continuuity.data.operation.executor.omid.QueueInvalidate.QueueUnack;
import com.continuuity.data.operation.executor.omid.QueueInvalidate.QueueUnenqueue;
import com.continuuity.data.operation.executor.omid.memory.MemoryRowSet;
import com.continuuity.data.operation.ttqueue.*;
import com.continuuity.data.operation.ttqueue.DequeueResult.DequeueStatus;
import com.continuuity.data.operation.ttqueue.QueueAdmin.GetGroupID;
import com.continuuity.data.operation.ttqueue.QueueAdmin.GetQueueMeta;
import com.continuuity.data.operation.ttqueue.QueueAdmin.QueueMeta;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.ReadPointer;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

/**
 * Implementation of an {@link com.continuuity.data.operation.executor.OperationExecutor}
 * that executes all operations within Omid-style transactions.
 *
 * See https://github.com/yahoo/omid/ for more information on the Omid design.
 */
@Singleton
public class OmidTransactionalOperationExecutor
implements TransactionalOperationExecutor {

  public String getName() {
    return "omid(" + tableHandle.getName() + ")";
  }

  /**
   * The Transaction Oracle used by this executor instance.
   */
  @Inject
  TransactionOracle oracle;

  /**
   * The {@link OVCTableHandle} handle used to get references to tables.
   */
  @Inject
  OVCTableHandle tableHandle;

  private OrderedVersionedColumnarTable randomTable;

  @SuppressWarnings("unused")
  private OrderedVersionedColumnarTable orderedTable;

  private TTQueueTable queueTable;

  private TTQueueTable streamTable;

  static int MAX_DEQUEUE_RETRIES = 200;
  static long DEQUEUE_RETRY_SLEEP = 5;

  // Single reads

  @Override
  public byte[] execute(ReadKey read) {
    initialize();
    byte [] result = read(read, this.oracle.getReadPointer());
    return result;
  }

  byte [] read(ReadKey read, ReadPointer pointer) {
    return this.randomTable.get(read.getKey(), Operation.KV_COL, pointer);
  }

  @Override
  public List<byte[]> execute(ReadAllKeys readKeys) {
    initialize();
    List<byte[]> result = this.randomTable.getKeys(readKeys.getLimit(),
        readKeys.getOffset(), this.oracle.getReadPointer());
    return result;
  }

  @Override
  public Map<byte[], byte[]> execute(Read read) {
    initialize();
    Map<byte[],byte[]> result = this.randomTable.get(read.getKey(),
        read.getColumns(), this.oracle.getReadPointer());
    return result;
  }

  @Override
  public Map<byte[], byte[]> execute(ReadColumnRange readColumnRange) {
    initialize();
    Map<byte[],byte[]> result = this.randomTable.get(readColumnRange.getKey(),
        readColumnRange.getStartColumn(), readColumnRange.getStopColumn(),
        this.oracle.getReadPointer());
    return result;
  }

  // Administrative calls

  @Override
  public void execute(ClearFabric clearFabric) {
    initialize();
    if (clearFabric.shouldClearData()) this.randomTable.clear();
    if (clearFabric.shouldClearQueues()) this.queueTable.clear();
    if (clearFabric.shouldClearStreams()) this.streamTable.clear();
  }

  // Write batches

  @Override
  public BatchOperationResult execute(List<WriteOperation> writes)
      throws OmidTransactionException {
    initialize();
    return execute(writes, startTransaction());
  }

  private boolean executeAsBatch(WriteOperation write)
      throws OmidTransactionException {
    List<WriteOperation> writes = Arrays.asList(write);
    return execute(writes).isSuccess();
  }

  BatchOperationResult execute(List<WriteOperation> writes,
      ImmutablePair<ReadPointer,Long> pointer)
          throws OmidTransactionException {

    if (writes.isEmpty()) return new BatchOperationResult(true, "Empty query");

    // Re-order operations (create a copy for now)
    List<WriteOperation> orderedWrites = new ArrayList<WriteOperation>(writes);
    Collections.sort(orderedWrites, new WriteOperationComparator());

    // Execute operations
    RowSet rows = new MemoryRowSet();
    List<Delete> deletes = new ArrayList<Delete>(writes.size());
    List<QueueInvalidate> invalidates = new ArrayList<QueueInvalidate>();
    for (WriteOperation write : orderedWrites) {
      WriteTransactionResult writeTxReturn = dispatchWrite(write, pointer);
      if (!writeTxReturn.success) {
        // Write operation failed
        abortTransaction(pointer, deletes, invalidates);
        return new BatchOperationResult(false,
            "A write operation failed, transaction aborted");
      } else {
        // Write was successful.  Store delete if we need to abort and continue
        deletes.addAll(writeTxReturn.deletes);
        if (writeTxReturn.invalidate != null) {
          // Queue operation
          invalidates.add(writeTxReturn.invalidate);
        } else {
          // Normal write operation
          rows.addRow(write.getKey());
        }
      }
    }

    // All operations completed successfully, commit transaction
    if (!commitTransaction(pointer, rows)) {
      // Commit failed, abort
      abortTransaction(pointer, deletes, invalidates);
      return new BatchOperationResult(false,
          "Commit of transaction failed, transaction aborted");
    }

    // If last operation was an ack, finalize it
    if (orderedWrites.get(orderedWrites.size() - 1) instanceof QueueAck) {
      QueueAck ack = (QueueAck)orderedWrites.get(orderedWrites.size() - 1);
      QueueFinalize finalize = new QueueFinalize(ack.getKey(),
          ack.getEntryPointer(), ack.getConsumer(), ack.getNumGroups());
      finalize.execute(getQueueTable(ack.getKey()), pointer);
    }

    // Transaction was successfully committed
    return new BatchOperationResult(true);
  }

  public OVCTableHandle getTableHandle() {
    return this.tableHandle;
  }

  private class WriteTransactionResult {
    final boolean success;
    final List<Delete> deletes;
    final QueueInvalidate invalidate;
    WriteTransactionResult(boolean success) {
      this(success, new ArrayList<Delete>());
    }
    WriteTransactionResult(boolean success, Delete delete) {
      this(success, Arrays.asList(new Delete [] { delete } ));
    }
    WriteTransactionResult(boolean success, List<Delete> deletes) {
      this(success, deletes, null);
    }
    public WriteTransactionResult(boolean success,
        QueueInvalidate invalidate) {
      this(success, new ArrayList<Delete>(), invalidate);
    }
    WriteTransactionResult(boolean success, List<Delete> deletes,
        QueueInvalidate invalidate) {
      this.success = success;
      this.deletes = deletes;
      this.invalidate = invalidate;
    }
  }
  /**
   * Actually perform the various write operations.
   * @param write
   * @param pointer
   * @return
   */
  private WriteTransactionResult dispatchWrite(
      WriteOperation write, ImmutablePair<ReadPointer,Long> pointer) {
    if (write instanceof Write) {
      return write((Write)write, pointer);
    } else if (write instanceof Delete) {
      return write((Delete)write, pointer);
    } else if (write instanceof Increment) {
      return write((Increment)write, pointer);
    } else if (write instanceof CompareAndSwap) {
      return write((CompareAndSwap)write, pointer);
    } else if (write instanceof QueueEnqueue) {
      return write((QueueEnqueue)write, pointer);
    } else if (write instanceof QueueAck) {
      return write((QueueAck)write, pointer);
    }
    return new WriteTransactionResult(false);
  }

  WriteTransactionResult write(Write write,
      ImmutablePair<ReadPointer,Long> pointer) {
    initialize();
    this.randomTable.put(write.getKey(), write.getColumns(),
        pointer.getSecond(), write.getValues());
    return new WriteTransactionResult(true,
        new Delete(write.getKey(), write.getColumns()));
  }

  WriteTransactionResult write(Delete delete,
      ImmutablePair<ReadPointer, Long> pointer) {
    initialize();
    this.randomTable.deleteAll(delete.getKey(), delete.getColumns(),
        pointer.getSecond());
    return new WriteTransactionResult(true,
        new Undelete(delete.getKey(), delete.getColumns()));
  }

  WriteTransactionResult write(Increment increment,
      ImmutablePair<ReadPointer,Long> pointer) {
    initialize();
    Map<byte[],Long> map = this.randomTable.increment(increment.getKey(),
        increment.getColumns(), increment.getAmounts(),
        pointer.getFirst(), pointer.getSecond());
    List<Delete> deletes = new ArrayList<Delete>(1);
    deletes.add(new Delete(increment.getKey(), increment.getColumns()));
    return new WriteTransactionResult(true, deletes);
  }

  WriteTransactionResult write(CompareAndSwap write,
      ImmutablePair<ReadPointer,Long> pointer) {
    initialize();
    boolean casReturn = this.randomTable.compareAndSwap(write.getKey(),
        write.getColumn(), write.getExpectedValue(), write.getNewValue(),
        pointer.getFirst(), pointer.getSecond());
    return new WriteTransactionResult(casReturn,
        new Delete(write.getKey(), write.getColumn()));
  }

  // TTQueues

  /**
   * Enqueue operations always succeed but can be rolled back.
   *
   * They are rolled back with an invalidate.
   *
   * @param pointer
   * @return
   */
  WriteTransactionResult write(QueueEnqueue enqueue,
      ImmutablePair<ReadPointer, Long> pointer) {
    initialize();
    EnqueueResult result = getQueueTable(enqueue.getKey()).enqueue(
        enqueue.getKey(), enqueue.getData(), pointer.getSecond());
    return new WriteTransactionResult(true,
        new QueueUnenqueue(enqueue.getKey(), result.getEntryPointer()));
  }

  WriteTransactionResult write(QueueAck ack,
      ImmutablePair<ReadPointer, Long> pointer) {
    initialize();
    boolean result = getQueueTable(ack.getKey()).ack(ack.getKey(),
        ack.getEntryPointer(), ack.getConsumer());
    if (!result) {
      // Ack failed, roll back transaction
      return new WriteTransactionResult(false);
    }
    return new WriteTransactionResult(true,
        new QueueUnack(ack.getKey(), ack.getEntryPointer(), ack.getConsumer()));
  }

  @Override
  public DequeueResult execute(QueueDequeue dequeue) {
    initialize();
    int retries = 0;
    long start = System.currentTimeMillis();
    TTQueueTable queueTable = getQueueTable(dequeue.getKey());
    while (retries < MAX_DEQUEUE_RETRIES) {
      DequeueResult result = queueTable.dequeue(dequeue.getKey(),
          dequeue.getConsumer(), dequeue.getConfig(),
          this.oracle.getReadPointer());
      if (result.shouldRetry()) {
        retries++;
        try {
          if (DEQUEUE_RETRY_SLEEP > 0) Thread.sleep(DEQUEUE_RETRY_SLEEP);
        } catch (InterruptedException e) {
          e.printStackTrace();
          // continue in loop
        }
        continue;
      }
      return result;
    }
    long end = System.currentTimeMillis();
    return new DequeueResult(DequeueStatus.FAILURE,
        "Maximum retries (retried " + retries + " times over " + (end-start) +
        " millis");
  }

  @Override
  public long execute(GetGroupID getGroupId) {
    initialize();
    TTQueueTable table = getQueueTable(getGroupId.getQueueName());
    long groupid = table.getGroupID(getGroupId.getQueueName());
    return groupid;
  }

  @Override
  public QueueMeta execute(GetQueueMeta getQueueMeta) {
    initialize();
    TTQueueTable table = getQueueTable(getQueueMeta.getQueueName());
    QueueMeta queueMeta = table.getQueueMeta(getQueueMeta.getQueueName());
    return queueMeta;
  }

  ImmutablePair<ReadPointer, Long> startTransaction() {
    return this.oracle.getNewPointer();
  }

  boolean commitTransaction(ImmutablePair<ReadPointer, Long> pointer,
      RowSet rows) throws OmidTransactionException {
    return this.oracle.commit(pointer.getSecond(), rows);
  }

  /**
   * Accessor to our Transaction Oracle instance
   * @return
   */
  TransactionOracle getOracle() {
    if (this.oracle == null) {
      throw new IllegalStateException("'oracle' field is null");
    }
    return this.oracle;
  }

  private void abortTransaction(ImmutablePair<ReadPointer,Long> pointer,
      List<Delete> deletes, List<QueueInvalidate> invalidates)
          throws OmidTransactionException {
    // Perform queue invalidates
    for (QueueInvalidate invalidate : invalidates) {
      invalidate.execute(getQueueTable(invalidate.queueName), pointer);
    }
    // Perform deletes
    for (Delete delete : deletes) {
      assert(delete != null);
      if (delete instanceof Undelete) {
        this.randomTable.undeleteAll(delete.getKey(), delete.getColumns(),
            pointer.getSecond());
      } else {
        this.randomTable.delete(delete.getKey(), delete.getColumns(),
            pointer.getSecond());
      }
    }
    // Notify oracle
    this.oracle.aborted(pointer.getSecond());
  }

  // Single Write Operations (Wrapped and called in a transaction batch)

  @SuppressWarnings("unused")
  private void unsupported(String msg) {
    throw new RuntimeException(msg);
  }

  @Override
  public boolean execute(Write write) {
    try {
      return executeAsBatch(write);
    } catch (OmidTransactionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean execute(Delete delete) {
    try {
      return executeAsBatch(delete);
    } catch (OmidTransactionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean execute(Increment inc) {
    try {
      return executeAsBatch(inc);
    } catch (OmidTransactionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean execute(CompareAndSwap cas) {
    try {
      return executeAsBatch(cas);
    } catch (OmidTransactionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean execute(QueueAck ack) {
    try {
      return executeAsBatch(ack);
    } catch (OmidTransactionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean execute(QueueEnqueue enqueue) {
    try {
      return executeAsBatch(enqueue);
    } catch (OmidTransactionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private TTQueueTable getQueueTable(byte[] queueName) {
    if (Bytes.startsWith(queueName, TTQueue.QUEUE_NAME_PREFIX))
      return this.queueTable;
    if (Bytes.startsWith(queueName, TTQueue.STREAM_NAME_PREFIX))
      return this.streamTable;
    // by default, use queue table
    return this.queueTable;
  }

  /**
   * A utility method that ensures this class is properly initialized before
   * it can be used. This currently entails creating real objects for all
   * our table handlers.
   */
  private synchronized void initialize() {

    if (this.randomTable == null) {

      this.randomTable = this.tableHandle.getTable(Bytes.toBytes("random"));
      this.orderedTable = this.tableHandle.getTable(Bytes.toBytes("ordered"));
      this.queueTable = this.tableHandle.getQueueTable(Bytes.toBytes("queues"));
      this.streamTable = this.tableHandle.getStreamTable(Bytes.toBytes("streams"));
    }
  }

} // end of OmitTransactionalOperationExecutor
