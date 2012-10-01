/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.operation.executor.omid;

import com.continuuity.api.data.*;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.Undelete;
import com.continuuity.data.operation.WriteOperationComparator;
import com.continuuity.data.operation.executor.TransactionalOperationExecutor;
import com.continuuity.data.operation.executor.omid.QueueInvalidate.QueueFinalize;
import com.continuuity.data.operation.executor.omid.QueueInvalidate.QueueUnack;
import com.continuuity.data.operation.executor.omid.QueueInvalidate.QueueUnenqueue;
import com.continuuity.data.operation.executor.omid.memory.MemoryRowSet;
import com.continuuity.data.operation.ttqueue.*;
import com.continuuity.data.operation.ttqueue.QueueAdmin.GetGroupID;
import com.continuuity.data.operation.ttqueue.QueueAdmin.GetQueueMeta;
import com.continuuity.data.operation.ttqueue.QueueAdmin.QueueMeta;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.ReadPointer;
import com.continuuity.metrics2.api.CMetrics;
import com.continuuity.metrics2.collector.MetricType;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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

  private TTQueueTable queueTable;
  private TTQueueTable streamTable;

  static int MAX_DEQUEUE_RETRIES = 200;
  static long DEQUEUE_RETRY_SLEEP = 5;

  // Metrics

  private CMetrics cmetric = new CMetrics(MetricType.System);
  
  private static final String METRIC_PREFIX = "omid-opex-";
  
  private void requestMetric(String requestType) {
    cmetric.meter(METRIC_PREFIX + requestType + "-numops", 1);
  }
  
  private long begin() { return System.currentTimeMillis(); }
  private void end(String requestType, long beginning) {
    cmetric.histogram(METRIC_PREFIX + requestType + "-latency",
        System.currentTimeMillis() - beginning);
  }
  
  // named table management

  // a map of logical table name to existing <real name, table>, caches
  // the meta data store and the ovc table handle
  // there are three possible states for a table:
  // 1. table does not exist or is not known -> no entry
  // 2. table is being created -> entry with real name, but null for the table
  // 3. table is known -> entry with name and table
  // Map<String,ImmutablePair<byte[],OrderedVersionedColumnarTable>>
  //    randomTables;

  // method to find - and if necessary create - a table
  /*
  OrderedVersionedColumnarTable findRandomTable(String name) {
    // if name is null, return default random table
    // look up table in in-memory map.
    // if (entry, table) return table
    // (A) if (entry, null) wait until table not null, return table
    // if null
    //   (B) read meta data for table
    //   if (real name, true) return getTable()
    //   if (real name, false) l
    //     loop and wait until created
    //     getTable()
    //     update in-memory table with table
    //     return
    //   if (null)
    //     generate unique name
    //     add (unique name, null) to in-memory table
    //     if fails, then someone else just created -> go back to  (A)
    //     add (unique name, false) to meta data and
    //     if fails, then someone else just created -> go back to (B)
    //     createNewTable(uniqueName) - should never fail
    //     update meta data (unique name, true)
    //     update in-memory table
    //     return table
  }
  */

  // Single reads

  @Override
  public OperationResult<byte[]> execute(OperationContext context,
                                         ReadKey read)
      throws OperationException {
    initialize();
    requestMetric("ReadKey");
    long begin = begin();
    OperationResult<byte[]> result = read(read, this.oracle.getReadPointer());
    end("ReadKey", begin);
    return result;
  }

  OperationResult<byte[]> read(ReadKey read, ReadPointer pointer)
      throws OperationException {
    return this.randomTable.get(read.getKey(), Operation.KV_COL, pointer);
  }

  @Override
  public OperationResult<List<byte[]>> execute(OperationContext context,
                                               ReadAllKeys readKeys)
      throws OperationException {
    initialize();
    requestMetric("ReadAllKeys");
    long begin = begin();
    List<byte[]> result = this.randomTable.getKeys(readKeys.getLimit(),
        readKeys.getOffset(), this.oracle.getReadPointer());
    end("ReadKey", begin);
    return new OperationResult<List<byte[]>>(result);
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context,
                                                      Read read)
      throws OperationException {
    initialize();
    requestMetric("Read");
    long begin = begin();
    OperationResult<Map<byte[], byte[]>> result = this.randomTable.get(
        read.getKey(), read.getColumns(), this.oracle.getReadPointer());
    end("Read", begin);
    return result;
  }

  @Override
  public OperationResult<Map<byte[], byte[]>>
  execute(OperationContext context,
          ReadColumnRange readColumnRange) throws OperationException {
    initialize();
    requestMetric("ReadColumnRange");
    long begin = begin();
    OperationResult<Map<byte[], byte[]>> result = this.randomTable.get(
        readColumnRange.getKey(), readColumnRange.getStartColumn(),
        readColumnRange.getStopColumn(), this.oracle.getReadPointer());
    end("ReadColumnRange", begin);
    return result;
  }

  // Administrative calls

  @Override
  public void execute(OperationContext context,
                      ClearFabric clearFabric) throws OperationException {
    initialize();
    requestMetric("ClearFabric");
    long begin = begin();
    if (clearFabric.shouldClearData()) this.randomTable.clear();
    if (clearFabric.shouldClearQueues()) this.queueTable.clear();
    if (clearFabric.shouldClearStreams()) this.streamTable.clear();
    end("ClearFabric", begin);
  }

  // Write batches

  @Override
  public void execute(OperationContext context,
                      List<WriteOperation> writes)
      throws OperationException {
    initialize();
    requestMetric("WriteOperationBatch");
    long begin = begin();
    cmetric.meter(METRIC_PREFIX + "WriteOperationBatch_NumReqs", writes.size());
    execute(writes, startTransaction());
    end("WriteOperationBatch", begin);
  }

  private void executeAsBatch(OperationContext context,
                              WriteOperation write)
      throws OperationException {
    execute(context, Collections.singletonList(write));
  }

  void execute(List<WriteOperation> writes,
               ImmutablePair<ReadPointer,Long> pointer)
      throws OperationException {

    if (writes.isEmpty()) return;

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
        cmetric.meter(METRIC_PREFIX + "WriteOperationBatch_FailedWrites", 1);
        abortTransaction(pointer, deletes, invalidates);
        throw new OmidTransactionException(
            writeTxReturn.statusCode, writeTxReturn.message);
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
      cmetric.meter(METRIC_PREFIX + "WriteOperationBatch_FailedCommits", 1);
      abortTransaction(pointer, deletes, invalidates);
      throw new OmidTransactionException(StatusCode.WRITE_CONFLICT,
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
    cmetric.meter(
        METRIC_PREFIX + "WriteOperationBatch_SuccessfulTransactions", 1);
  }

  public OVCTableHandle getTableHandle() {
    return this.tableHandle;
  }

  static final List<Delete> noDeletes = Collections.emptyList();

  private class WriteTransactionResult {
    final boolean success;
    final int statusCode;
    final String message;
    final List<Delete> deletes;
    final QueueInvalidate invalidate;

    WriteTransactionResult(boolean success, int status, String message,
                           List<Delete> deletes, QueueInvalidate invalidate) {
      this.success = success;
      this.statusCode = status;
      this.message = message;
      this.deletes = deletes;
      this.invalidate = invalidate;
    }

    // successful, one delete to undo
    WriteTransactionResult(Delete delete) {
      this(true, StatusCode.OK, null, Collections.singletonList(delete), null);
    }

    // successful, one queue operation to invalidate
    WriteTransactionResult(QueueInvalidate invalidate) {
      this(true, StatusCode.OK, null, noDeletes, invalidate);
    }

    // failure with status code and message, nothing to undo
    WriteTransactionResult(int status, String message) {
      this(false, status, message, noDeletes, null);
    }
  }

  /**
   * Actually perform the various write operations.
   */
  private WriteTransactionResult dispatchWrite(
      WriteOperation write, ImmutablePair<ReadPointer,Long> pointer) throws OperationException {
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
    return new WriteTransactionResult(StatusCode.INTERNAL_ERROR,
        "Unknown write operation " + write.getClass().getName());
  }

  WriteTransactionResult write(Write write,
      ImmutablePair<ReadPointer,Long> pointer) throws OperationException {
    initialize();
    requestMetric("Write");
    long begin = begin();
    this.randomTable.put(write.getKey(), write.getColumns(),
        pointer.getSecond(), write.getValues());
    end("Write", begin);
    return new WriteTransactionResult(
        new Delete(write.getKey(), write.getColumns()));
  }

  WriteTransactionResult write(Delete delete,
      ImmutablePair<ReadPointer, Long> pointer) throws OperationException {
    initialize();
    requestMetric("Delete");
    long begin = begin();
    this.randomTable.deleteAll(delete.getKey(), delete.getColumns(),
        pointer.getSecond());
    end("Delete", begin);
    return new WriteTransactionResult(
        new Undelete(delete.getKey(), delete.getColumns()));
  }

  WriteTransactionResult write(Increment increment,
      ImmutablePair<ReadPointer,Long> pointer) throws OperationException {
    initialize();
    requestMetric("Increment");
    long begin = begin();
    try {
      @SuppressWarnings("unused")
      Map<byte[],Long> map = this.randomTable.increment(increment.getKey(),
          increment.getColumns(), increment.getAmounts(),
          pointer.getFirst(), pointer.getSecond());
    } catch (OperationException e) {
      return new WriteTransactionResult(e.getStatus(), e.getMessage());
    }
    end("Increment", begin);
    return new WriteTransactionResult(
        new Delete(increment.getKey(), increment.getColumns()));
  }

  WriteTransactionResult write(CompareAndSwap write,
      ImmutablePair<ReadPointer,Long> pointer) throws OperationException {
    initialize();
    requestMetric("CompareAndSwap");
    long begin = begin();
    try {
      this.randomTable.compareAndSwap(write.getKey(),
          write.getColumn(), write.getExpectedValue(), write.getNewValue(),
          pointer.getFirst(), pointer.getSecond());
    } catch (OperationException e) {
      return new WriteTransactionResult(e.getStatus(), e.getMessage());
    }
    end("CompareAndSwap", begin);
    return new WriteTransactionResult(
        new Delete(write.getKey(), write.getColumn()));
  }

  // TTQueues

  /**
   * EnqueuePayload operations always succeed but can be rolled back.
   *
   * They are rolled back with an invalidate.
   */
  WriteTransactionResult write(QueueEnqueue enqueue,
      ImmutablePair<ReadPointer, Long> pointer) throws OperationException {
    initialize();
    requestMetric("QueueEnqueue");
    long begin = begin();
    EnqueueResult result = getQueueTable(enqueue.getKey()).enqueue(
        enqueue.getKey(), enqueue.getData(), pointer.getSecond());
    end("QueueEnqueue", begin);
    return new WriteTransactionResult(
        new QueueUnenqueue(enqueue.getKey(), result.getEntryPointer()));
  }

  WriteTransactionResult write(QueueAck ack,
      @SuppressWarnings("unused")
      ImmutablePair<ReadPointer, Long> pointer) throws OperationException {
    initialize();
    requestMetric("QueueAck");
    long begin = begin();
    try {
      getQueueTable(ack.getKey()).ack(ack.getKey(),
          ack.getEntryPointer(), ack.getConsumer());
    } catch (OperationException e) {
      // Ack failed, roll back transaction
      return new WriteTransactionResult(StatusCode.ILLEGAL_ACK,
          "Attempt to ack a dequeue of a different consumer");
    } finally {
      end("QueueAck", begin);
    }
    return new WriteTransactionResult(
        new QueueUnack(ack.getKey(), ack.getEntryPointer(), ack.getConsumer()));
  }

  @Override
  public DequeueResult execute(OperationContext context,
                               QueueDequeue dequeue)
      throws OperationException {
    initialize();
    requestMetric("QueueDequeue");
    long begin = begin();
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
      end("QueueDequeue", begin);
      return result;
    }
    long end = System.currentTimeMillis();
    end("QueueDequeue", begin);
    throw new OperationException(StatusCode.TOO_MANY_RETRIES,
        "Maximum retries (retried " + retries + " times over " + (end-start) +
        " millis");
  }

  @Override
  public long execute(OperationContext context,
                      GetGroupID getGroupId)
      throws OperationException {
    initialize();
    requestMetric("GetGroupID");
    long begin = begin();
    TTQueueTable table = getQueueTable(getGroupId.getQueueName());
    long groupid = table.getGroupID(getGroupId.getQueueName());
    end("GetGroupID", begin);
    return groupid;
  }

  @Override
  public OperationResult<QueueMeta> execute(OperationContext context,
                                            GetQueueMeta getQueueMeta)
      throws OperationException {
    initialize();
    requestMetric("GetQueueMeta");
    long begin = begin();
    TTQueueTable table = getQueueTable(getQueueMeta.getQueueName());
    QueueMeta queueMeta = table.getQueueMeta(getQueueMeta.getQueueName());
    end("GetQueueMeta", begin);
    return new OperationResult<QueueMeta>(queueMeta);
  }

  ImmutablePair<ReadPointer, Long> startTransaction() {
    requestMetric("StartTransaction");
    return this.oracle.getNewPointer();
  }

  boolean commitTransaction(ImmutablePair<ReadPointer, Long> pointer,
      RowSet rows) throws OmidTransactionException {
    requestMetric("CommitTransaction");
    return this.oracle.commit(pointer.getSecond(), rows);
  }

  private void abortTransaction(ImmutablePair<ReadPointer,Long> pointer,
      List<Delete> deletes, List<QueueInvalidate> invalidates)
      throws OperationException {
    // Perform queue invalidates
    cmetric.meter(METRIC_PREFIX + "WriteOperationBatch_AbortedTransactions", 1);
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
  public void execute(OperationContext context,
                      Write write) throws OperationException {
    executeAsBatch(context, write);
  }

  @Override
  public void execute(OperationContext context,
                      Delete delete) throws OperationException {
    executeAsBatch(context, delete);
  }

  @Override
  public void execute(OperationContext context,
                      Increment inc) throws OperationException {
    executeAsBatch(context, inc);
  }

  @Override
  public void execute(OperationContext context,
                      CompareAndSwap cas) throws OperationException {
    executeAsBatch(context, cas);
  }

  @Override
  public void execute(OperationContext context,
                      QueueAck ack) throws OperationException {
    executeAsBatch(context, ack);
  }

  @Override
  public void execute(OperationContext context,
                      QueueEnqueue enqueue) throws OperationException {
    executeAsBatch(context, enqueue);
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
  private synchronized void initialize() throws OperationException {

    if (this.randomTable == null) {

      this.randomTable = this.tableHandle.getTable(Bytes.toBytes("random"));
      this.queueTable = this.tableHandle.getQueueTable(Bytes.toBytes("queues"));
      this.streamTable = this.tableHandle.getStreamTable(Bytes.toBytes("streams"));
    }
  }

} // end of OmitTransactionalOperationExecutor
