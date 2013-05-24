package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.continuuity.data.operation.ttqueue.EnqueueResult.EnqueueStatus;
import com.continuuity.data.operation.ttqueue.admin.QueueInfo;
import com.continuuity.hbase.ttqueue.HBQAck;
import com.continuuity.hbase.ttqueue.HBQDequeue;
import com.continuuity.hbase.ttqueue.HBQDequeueResult;
import com.continuuity.hbase.ttqueue.HBQEnqueue;
import com.continuuity.hbase.ttqueue.HBQEnqueueResult;
import com.continuuity.hbase.ttqueue.HBQExpirationConfig;
import com.continuuity.hbase.ttqueue.HBQFinalize;
import com.continuuity.hbase.ttqueue.HBQInvalidate;
import com.continuuity.hbase.ttqueue.HBQMetaOperation;
import com.continuuity.hbase.ttqueue.HBQMetaOperation.MetaOperationType;
import com.continuuity.hbase.ttqueue.HBQQueueMeta;
import com.continuuity.hbase.ttqueue.HBQShardConfig;
import com.continuuity.hbase.ttqueue.HBQUnack;
import com.continuuity.hbase.ttqueue.HBReadPointer;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of a single {@link TTQueue} on an HBase table using native
 * HBase queue operations and a multi-row, sharded schema.
 */
public class TTQueueOnHBaseNative implements TTQueue {

  private final HTable table;
  private final byte [] queueName;
  final TransactionOracle oracle;

  HBQShardConfig shardConfig;
  HBQExpirationConfig expirationConfig;

  // For testing
  AtomicLong dequeueReturns = new AtomicLong(0);

  /**
   * Constructs a TTQueue with the specified queue name, backed by the specified
   * HBase table, and utilizing the specified time oracle to generate stamps for
   * dirty reads and writes.  Utilizes specified Configuration to determine
   * shard maximums.
   */
  public TTQueueOnHBaseNative(final HTable table, final byte [] queueName,
      final TransactionOracle oracle, final CConfiguration conf) {
    this.table = table;
    this.queueName = queueName;
    this.oracle = oracle;
    this.shardConfig = new HBQShardConfig(
        conf.getLong("ttqueue.shard.max.entries", 1024),
        conf.getLong("ttqueue.shard.max.bytes", 1024*1024*1024)); // 1GB
    this.expirationConfig = new HBQExpirationConfig(
        conf.getLong("ttqueue.entry.age.max", 120 * 1000), // 120 seconds
        conf.getLong("ttqueue.entry.semiacked.max", 10 * 1000)); // 10 seconds
  }

  @Override
  public EnqueueResult enqueue(QueueEntry[] entries, Transaction transaction) throws OperationException {
    if (entries.length == 1) {
      return enqueue(entries[0], transaction);
    } else {
      throw new RuntimeException("Old queues don't support batch - received batch size of " + entries.length);
    }
  }

  @Override
  public EnqueueResult enqueue(QueueEntry entry, Transaction transaction)
      throws OperationException {
    if (TRACE)
      log("Enqueueing (data.len=" + entry.getData().length + ", transaction=" +
          transaction + ")");

    // Get a read pointer that sees everything (dirty read pointer)
    long dirtyReadVersion = TransactionOracle.DIRTY_READ_POINTER.getMaximum();
    // Perform native enqueue operation
    HBQEnqueueResult result;
    try {
      result = this.table.enqueue(
          new HBQEnqueue(this.queueName, entry.getData(),
              new HBReadPointer(transaction.getWriteVersion(), dirtyReadVersion), this.shardConfig));
    } catch (IOException e) {
      log("HBase exception: " + e.getMessage());
      e.printStackTrace();
      throw new OperationException(StatusCode.HBASE_ERROR, e.getMessage());
    }
    return new EnqueueResult(EnqueueStatus.SUCCESS,
        new QueueEntryPointer(this.queueName,
            result.getEntryPointer().getEntryId(),
            result.getEntryPointer().getShardId()));
  }

  @Override
  public void invalidate(QueueEntryPointer[] entryPointers, Transaction transaction) throws OperationException {
    if (entryPointers.length == 1) {
      invalidate(entryPointers[0], transaction.getWriteVersion());
    } else {
      throw new RuntimeException("Old queues don't support batch - received batch size of " + entryPointers.length);
    }
  }

  public void invalidate(QueueEntryPointer entryPointer,
      long cleanWriteVersion) throws OperationException {
    if (TRACE) log("Invalidating " + entryPointer);
    long dirtyReadVersion = TransactionOracle.DIRTY_READ_POINTER.getMaximum();
    try {
      this.table.invalidate(new HBQInvalidate(this.queueName,
          entryPointer.toHBQ(), new HBReadPointer(cleanWriteVersion, dirtyReadVersion)));
    } catch (IOException e) {
      log("HBase exception: " + e.getMessage());
      e.printStackTrace();
      throw new OperationException(StatusCode.HBASE_ERROR, e.getMessage());
    }
  }

  @Override
  public DequeueResult dequeue(QueueConsumer consumer, ReadPointer readPointer) throws OperationException {
    return dequeueInternal(consumer, consumer.getQueueConfig(), readPointer);
  }

  private DequeueResult dequeueInternal(QueueConsumer consumer, QueueConfig config,
      ReadPointer readPointer) throws OperationException {

    if (TRACE)
      log("Attempting dequeue [curNumDequeues=" + this.dequeueReturns.get() +
          "] (" + consumer + ", " + config + ", " + readPointer + ")");

    // Get access to the clean read pointer and get a dirty read pointer
    MemoryReadPointer memoryPointer = (MemoryReadPointer)readPointer;

    // Perform native dequeue operation
    HBQDequeueResult dequeueResult;
    try {
      dequeueResult = this.table.dequeue(new HBQDequeue(
          this.queueName, consumer.toHBQ(), config.toHBQ(), new HBReadPointer(
              memoryPointer.getWriteVersion(), memoryPointer.getReadPointer(),
              memoryPointer.getReadExcludes()),
              this.expirationConfig));
    } catch (IOException e) {
      log("HBase exception: " + e.getMessage());
      e.printStackTrace();
      throw new OperationException(StatusCode.HBASE_ERROR, e.getMessage());
    }
    if (dequeueResult.isFailure()) {
      throw new OperationException(StatusCode.ILLEGAL_GROUP_CONFIG_CHANGE,
          "DequeuePayload failed (" + dequeueResult.getFailureMessage() + ")");
    }
    if (dequeueResult.isSuccess()) dequeueReturns.incrementAndGet();
    return new DequeueResult(this.queueName, dequeueResult);
  }

  @Override
  public void ack(QueueEntryPointer[] entryPointers, QueueConsumer consumer, Transaction transaction)
    throws OperationException {
    if (entryPointers.length == 1) {
      ack(entryPointers[0], consumer, transaction);
    } else {
      throw new RuntimeException("Old queues don't support batch - received batch size of " + entryPointers.length);
    }
  }

  @Override
  public void ack(QueueEntryPointer entryPointer, QueueConsumer consumer, Transaction transaction)
      throws OperationException {
    if (TRACE) log("Acking " + entryPointer);
    long dirtyWriteVersion = TransactionOracle.DIRTY_WRITE_VERSION;
    long dirtyReadVersion = TransactionOracle.DIRTY_READ_POINTER.getMaximum();
    try {
      if (!this.table.ack(new HBQAck(this.queueName, consumer.toHBQ(),
          entryPointer.toHBQ(), new HBReadPointer(dirtyWriteVersion, dirtyReadVersion)))) {
        throw new OperationException(StatusCode.ILLEGAL_ACK, "Ack failed");
      }
    } catch (IOException e) {
      log("HBase exception: " + e.getMessage());
      e.printStackTrace();
      throw new OperationException(StatusCode.HBASE_ERROR, e.getMessage());
    }
  }

  @Override
  public void finalize(QueueEntryPointer[] entryPointers, QueueConsumer consumer, int totalNumGroups,
                       Transaction transaction) throws OperationException {
    if (entryPointers.length == 1) {
      finalize(entryPointers[0], consumer, totalNumGroups, transaction);
    } else {
      throw new RuntimeException("Old queues don't support batch - received batch size of " + entryPointers.length);
    }
  }

  public void finalize(QueueEntryPointer entryPointer, QueueConsumer consumer, int totalNumGroups,
                       Transaction transaction) throws OperationException {
    if (TRACE) log("Finalizing " + entryPointer);
    long dirtyWriteVersion = TransactionOracle.DIRTY_WRITE_VERSION;
    long dirtyReadVersion = TransactionOracle.DIRTY_READ_POINTER.getMaximum();
    try {
      if (!this.table.finalize(new HBQFinalize(this.queueName, consumer.toHBQ(),
          entryPointer.toHBQ(), new HBReadPointer(dirtyWriteVersion, dirtyReadVersion), totalNumGroups))) {
        throw new OperationException(StatusCode.ILLEGAL_FINALIZE,
            "Finalize failed");
      }
    } catch (IOException e) {
      log("HBase exception: " + e.getMessage());
      e.printStackTrace();
      throw new OperationException(StatusCode.HBASE_ERROR, e.getMessage());
    }
  }

  @Override
  public void unack(QueueEntryPointer[] entryPointers, QueueConsumer consumer, Transaction transaction) throws
    OperationException {
    if (entryPointers.length == 1) {
      unack(entryPointers[0], consumer, transaction);
    } else {
      throw new RuntimeException("Old queues don't support batch - received batch size of " + entryPointers.length);
    }
  }

  public void unack(QueueEntryPointer entryPointer, QueueConsumer consumer,
                    @SuppressWarnings("unused") Transaction transaction) throws OperationException {
    if (TRACE) log("Unacking " + entryPointer);
    long dirtyWriteVersion = TransactionOracle.DIRTY_WRITE_VERSION;
    long dirtyReadVersion = TransactionOracle.DIRTY_READ_POINTER.getMaximum();
    try {
      if (!this.table.unack(new HBQUnack(this.queueName, consumer.toHBQ(),
          entryPointer.toHBQ(), new HBReadPointer(dirtyWriteVersion, dirtyReadVersion)))) {
        throw new OperationException(StatusCode.ILLEGAL_UNACK, "Unack failed");
      }
    } catch (IOException e) {
      log("HBase exception: " + e.getMessage());
      e.printStackTrace();
      throw new OperationException(StatusCode.HBASE_ERROR, e.getMessage());
    }
  }

  @Override
  public long getGroupID() throws OperationException {
    if (TRACE) log("GetGroupId");
    try {
      return this.table.getGroupID(new HBQMetaOperation(this.queueName,
          MetaOperationType.GET_GROUP_ID));
    } catch (IOException e) {
      log("HBase exception: " + e.getMessage());
      e.printStackTrace();
      throw new OperationException(StatusCode.HBASE_ERROR, e.getMessage());
    }
  }

  @Override
  public QueueInfo getQueueInfo() throws OperationException {
    if (TRACE) log("GetQueueInfo");
    try {
      HBQQueueMeta queueMeta = this.table.getQueueMeta(
          new HBQMetaOperation(this.queueName,
              MetaOperationType.GET_QUEUE_META));
      return new QueueInfo(queueMeta);
    } catch (IOException e) {
      log("HBase exception: " + e.getMessage());
      e.printStackTrace();
      throw new OperationException(StatusCode.HBASE_ERROR, e.getMessage());
    }
  }

  @Override
  public int configure(QueueConsumer newConsumer, ReadPointer readPointer)
    throws OperationException {
    // Noting to do, only needs to be implemented in com.continuuity.data.operation.ttqueue.TTQueueNewOnVCTable
    return -1;
  }

  @Override
  public List<Long> configureGroups(List<Long> groupIds) throws OperationException {
    // Noting to do, only needs to be implemented in com.continuuity.data.operation.ttqueue.TTQueueNewOnVCTable
    return Collections.emptyList();
  }

  @Override
  public void dropInflightState(QueueConsumer consumer, ReadPointer readPointer) throws OperationException {
    // Noting to do, only needs to be implemented in com.continuuity.data.operation.ttqueue.TTQueueNewOnVCTable
  }

// Private helpers

  public static boolean TRACE = false;

  private void log(String msg) {
    if (TRACE) System.out.println(Thread.currentThread().getId() + " : " + msg);
  }
}
