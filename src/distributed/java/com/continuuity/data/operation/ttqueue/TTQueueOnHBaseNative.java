package com.continuuity.data.operation.ttqueue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.continuuity.data.operation.ttqueue.EnqueueResult.EnqueueStatus;
import com.continuuity.data.operation.ttqueue.QueueAdmin.QueueMeta;
import com.continuuity.data.table.ReadPointer;
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

/**
 * Implementation of a single {@link TTQueue} on an HBase table using native
 * HBase queue operations and a multi-row, sharded schema.
 */
public class TTQueueOnHBaseNative implements TTQueue {

  private final HTable table;
  private final byte [] queueName;
  final TimestampOracle timeOracle;

  HBQShardConfig shardConfig;
  HBQExpirationConfig expirationConfig;

  // For testing
  AtomicLong dequeueReturns = new AtomicLong(0);

  /**
   * Constructs a TTQueue with the specified queue name, backed by the specified
   * HBase table, and utilizing the specified time oracle to generate stamps for
   * dirty reads and writes.  Utilizes specified Configuration to determine
   * shard maximums.
   * @param table
   * @param queueName
   * @param timeOracle
   * @param conf
   */
  public TTQueueOnHBaseNative(final HTable table, final byte [] queueName,
      final TimestampOracle timeOracle, final Configuration conf) {
    this.table = table;
    this.queueName = queueName;
    this.timeOracle = timeOracle;
    this.shardConfig = new HBQShardConfig(
        conf.getLong("ttqueue.shard.max.entries", 1024),
        conf.getLong("ttqueue.shard.max.bytes", 1024*1024*1024)); // 1MB
    this.expirationConfig = new HBQExpirationConfig(
        conf.getLong("ttqueue.entry.age.max", 120 * 1000), // 120 seconds
        conf.getLong("ttqueue.entry.semiacked.max", 10 * 1000)); // 10 seconds
  }

  @Override
  public EnqueueResult enqueue(byte[] data, long cleanWriteVersion)
      throws OperationException {
    if (TRACE)
      log("Enqueueing (data.len=" + data.length + ", writeVersion=" +
          cleanWriteVersion + ")");

    // Get a read pointer that sees everything (dirty read pointer)
    long now = this.timeOracle.getTimestamp();
    // Perform native enqueue operation
    HBQEnqueueResult result;
    try {
      result = this.table.enqueue(
          new HBQEnqueue(this.queueName, data,
              new HBReadPointer(cleanWriteVersion, now), this.shardConfig));
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
  public void invalidate(QueueEntryPointer entryPointer,
      long cleanWriteVersion) throws OperationException {
    if (TRACE) log("Invalidating " + entryPointer);
    long now = this.timeOracle.getTimestamp();
    try {
      this.table.invalidate(new HBQInvalidate(this.queueName,
          QueueToHBQ.toHBQ(entryPointer),
          new HBReadPointer(cleanWriteVersion, now)));
    } catch (IOException e) {
      log("HBase exception: " + e.getMessage());
      e.printStackTrace();
      throw new OperationException(StatusCode.HBASE_ERROR, e.getMessage());
    }
  }

  @Override
  public DequeueResult dequeue(QueueConsumer consumer, QueueConfig config,
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
          this.queueName, QueueToHBQ.toHBQ(consumer), QueueToHBQ.toHBQ(config),
          new HBReadPointer(memoryPointer.getWritePointer(),
              memoryPointer.getReadPointer(), memoryPointer.getReadExcludes()),
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
    return QueueToHBQ.fromHBQ(this.queueName, dequeueResult);
  }

  @Override
  public void ack(QueueEntryPointer entryPointer, QueueConsumer consumer)
      throws OperationException {
    if (TRACE) log("Acking " + entryPointer);
    long now = this.timeOracle.getTimestamp();
    try {
      if (!this.table.ack(new HBQAck(this.queueName, QueueToHBQ.toHBQ(consumer),
          QueueToHBQ.toHBQ(entryPointer), new HBReadPointer(now, now)))) {
        throw new OperationException(StatusCode.ILLEGAL_ACK, "Ack failed");
      }
    } catch (IOException e) {
      log("HBase exception: " + e.getMessage());
      e.printStackTrace();
      throw new OperationException(StatusCode.HBASE_ERROR, e.getMessage());
    }
  }

  @Override
  public void finalize(QueueEntryPointer entryPointer,
      QueueConsumer consumer, int totalNumGroups)
          throws OperationException {
    if (TRACE) log("Finalizing " + entryPointer);
    long now = this.timeOracle.getTimestamp();
    try {
      if (!this.table.finalize(new HBQFinalize(this.queueName,
          QueueToHBQ.toHBQ(consumer), QueueToHBQ.toHBQ(entryPointer),
          new HBReadPointer(now, now), totalNumGroups))) {
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
  public void unack(QueueEntryPointer entryPointer,
      QueueConsumer consumer) throws OperationException {
    if (TRACE) log("Unacking " + entryPointer);
    long now = this.timeOracle.getTimestamp();
    try {
      if (!this.table.unack(new HBQUnack(this.queueName,
          QueueToHBQ.toHBQ(consumer),QueueToHBQ.toHBQ(entryPointer),
          new HBReadPointer(now, now)))) {
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
  public QueueMeta getQueueMeta() throws OperationException {
    if (TRACE) log("GetQueueMeta");
    try {
      HBQQueueMeta queueMeta = this.table.getQueueMeta(
          new HBQMetaOperation(this.queueName,
              MetaOperationType.GET_QUEUE_META));
      return QueueToHBQ.fromHBQ(queueMeta);
    } catch (IOException e) {
      log("HBase exception: " + e.getMessage());
      e.printStackTrace();
      throw new OperationException(StatusCode.HBASE_ERROR, e.getMessage());
    }
  }

  // Private helpers

  public static boolean TRACE = false;

  private void log(String msg) {
    if (TRACE) System.out.println(Thread.currentThread().getId() + " : " + msg);
  }
}
