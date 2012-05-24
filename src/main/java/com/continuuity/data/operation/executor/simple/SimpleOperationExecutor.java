package com.continuuity.data.operation.executor.simple;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.SyncReadTimeoutException;
import com.continuuity.data.engine.SimpleQueueTable;
import com.continuuity.data.engine.SimpleTableHandle;
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
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.queue.QueueAck;
import com.continuuity.data.operation.queue.QueueEntry;
import com.continuuity.data.operation.queue.QueuePop;
import com.continuuity.data.operation.queue.QueuePush;
import com.continuuity.data.operation.type.WriteOperation;
import com.continuuity.data.table.SimpleTable;

public class SimpleOperationExecutor implements OperationExecutor {

  final SimpleTableHandle tableHandle;

  final SimpleTable randomTable;

  final SimpleTable orderedTable;

  final SimpleQueueTable queueTable;

  public SimpleOperationExecutor(SimpleTableHandle tableHandle) {
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
  public boolean execute(List<WriteOperation> writes) {
    for (WriteOperation write : writes) {
      if(!execute(write)) return false;
    }
    return true;
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
    } else if (write instanceof QueuePush) {
      if (!execute((QueuePush)write)) return false;
    } else if (write instanceof QueueAck) {
      if (!execute((QueueAck)write)) return false;
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
    if (delete.hasColumn()) {
      this.randomTable.delete(delete.getKey(), delete.getColumn());
    } else {
      this.randomTable.delete(delete.getKey());
    }
    return true;
  }

  @Override
  public boolean execute(QueuePush push) {
    return this.queueTable.push(push.getQueueName(), push.getValue());
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

  @Override
  public boolean execute(QueueAck ack) {
    return this.queueTable.ack(ack.getQueueName(), ack.getQueueEntry());
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
  public byte[] execute(Read read) throws SyncReadTimeoutException {
    return this.randomTable.get(read.getKey(), COLUMN);
  }

  @Override
  public long execute(ReadCounter readCounter) throws SyncReadTimeoutException {
    return this.randomTable.increment(readCounter.getKey(), COLUMN, 0);
  }

  @Override
  public QueueEntry execute(QueuePop pop) throws SyncReadTimeoutException,
      InterruptedException {
    return this.queueTable.pop(pop.getQueueName(), pop.getConsumer(),
        pop.getConfig(), pop.getDrain());
  }

  @Override
  public Map<byte[], byte[]> execute(OrderedRead orderedRead)
      throws SyncReadTimeoutException {
    throw new RuntimeException("Ordered operations not currently supported");
//    Map<byte[], byte[]> result = null;
//    if (orderedRead.getEndKey() == null) {
//      if (orderedRead.getLimit() <= 1) {
//        result = this.executor.readOrdered(orderedRead.getStartKey());
//      } else {
//        result = this.executor.readOrdered(orderedRead.getStartKey(),
//            orderedRead.getLimit());
//      }
//    } else {
//      result = this.executor.readOrdered(orderedRead.getStartKey(),
//          orderedRead.getEndKey());
//    }
//    orderedRead.setResult(result);
//    return result;
  }
}
