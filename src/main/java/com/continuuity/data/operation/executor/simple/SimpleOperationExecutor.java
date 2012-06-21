package com.continuuity.data.operation.executor.simple;

import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.api.data.CompareAndSwap;
import com.continuuity.api.data.Delete;
import com.continuuity.api.data.Increment;
import com.continuuity.api.data.OperationGenerator;
import com.continuuity.api.data.ReadKey;
import com.continuuity.api.data.ReadCounter;
import com.continuuity.api.data.ReadKeys;
import com.continuuity.api.data.SyncReadTimeoutException;
import com.continuuity.api.data.Write;
import com.continuuity.api.data.WriteOperation;
import com.continuuity.data.operation.OrderedWrite;
import com.continuuity.data.operation.ReadModifyWrite;
import com.continuuity.data.operation.executor.BatchOperationResult;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueAdmin.GetGroupID;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.TTQueueTable;
import com.continuuity.data.table.ColumnarTable;
import com.continuuity.data.table.ColumnarTableHandle;
import com.continuuity.data.table.OVCTableHandle;

public class SimpleOperationExecutor implements OperationExecutor {

  final ColumnarTableHandle tableHandle;

  final ColumnarTable randomTable;

  final ColumnarTable orderedTable;

  final TTQueueTable queueTable;

  public SimpleOperationExecutor(ColumnarTableHandle tableHandle) {
    this.tableHandle = tableHandle;
    this.randomTable = tableHandle.getTable(Bytes.toBytes("random"));
    this.orderedTable = tableHandle.getTable(Bytes.toBytes("ordered"));
    this.queueTable = tableHandle.getQueueTable(Bytes.toBytes("queues"));
  }

  // Batch of Writes

  /**
   * Performs the specified writes synchronously and in sequence.
   *
   * If an error is reached, execution of subsequent operations is skipped and
   * false is returned.  If all operations are performed successfully, returns
   * true.
   *
   * @param writes write operations to be performed in sequence
   * @return true if all operations succeeded, false if not
   */
  @Override
  public BatchOperationResult execute(List<WriteOperation> writes) {
    for (WriteOperation write : writes) {
      if(!execute(write)) return new BatchOperationResult(false,
          "Write operation failed");
    }
    return new BatchOperationResult(true);
  }

  private boolean execute(WriteOperation write) {
    if (write instanceof Write) {
      if (!execute((Write)write)) return false;
    } else if (write instanceof OrderedWrite) {
      if (!execute((OrderedWrite)write)) return false;
    } else if (write instanceof ReadModifyWrite) {
      if (!execute((ReadModifyWrite)write)) return false;
    } else if (write instanceof Increment) {
      if (!execute((Increment)write)) return false;
    } else if (write instanceof CompareAndSwap) {
      if (!execute((CompareAndSwap)write)) return false;
    }
    return true;
  }

  // Static Constants

  static final byte [] COLUMN = Bytes.toBytes("c");

  static final int MAX_RETRIES = 10;

  // Single Writes

  @Override
  public boolean execute(Write write) {
    this.randomTable.put(write.getKey(), COLUMN, write.getValue());
    return true;
  }

  @Override
  public boolean execute(Delete delete) {
    this.randomTable.delete(delete.getKey(), COLUMN);
    return true;
  }

  @Override
  public boolean execute(OrderedWrite write) {
    throw new RuntimeException("Ordered operations not currently supported");
  }

  // Conditional Writes

  @Override
  public boolean execute(CompareAndSwap cas) {
    return this.randomTable.compareAndSwap(cas.getKey(), COLUMN,
        cas.getExpectedValue(), cas.getNewValue());
  }

  // Value Returning Read-Modify-Writes

  @Override
  public boolean execute(Increment inc) {
    long amount =
        this.randomTable.increment(inc.getKey(), COLUMN, inc.getAmount());
    inc.setResult(amount);
    OperationGenerator<Long> generator =
        inc.getPostIncrementOperationGenerator();
    if (generator != null) {
      WriteOperation writeOperation = generator.generateWriteOperation(amount);
      if (writeOperation != null) {
        return execute(writeOperation);
      }
    }
    return true;
  }

  @Override
  public boolean execute(ReadModifyWrite rmw) {
    // retryable operation
    int retries = 0;
    while (retries++ < MAX_RETRIES) {
      byte [] existingValue = this.randomTable.get(rmw.getKey(), COLUMN);
      byte [] newValue = rmw.getModifier().modify(existingValue);
      if (this.randomTable.compareAndSwap(
          rmw.getKey(), COLUMN, existingValue, newValue)) {
        return true;
      }
    }
    return false;
  }

  // Simple Reads

  @Override
  public byte[] execute(ReadKey read) throws SyncReadTimeoutException {
    return this.randomTable.get(read.getKey(), COLUMN);
  }

  @Override
  public long execute(ReadCounter readCounter) throws SyncReadTimeoutException {
    return this.randomTable.increment(readCounter.getKey(), COLUMN, 0);
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
  public boolean execute(QueueAck ack) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(QueueEnqueue enqueue) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public OVCTableHandle getTableHandle() {
    return null;
  }

  @Override
  public List<byte[]> execute(ReadKeys readKeys)
      throws SyncReadTimeoutException {
    // TODO Auto-generated method stub
    return null;
  }
}
