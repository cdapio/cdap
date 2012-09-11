package com.continuuity.data.operation.ttqueue;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.hbase.ttqueue.HBQEntryPointer;
import com.google.common.base.Objects;

/**
 * A pointer which completely addresses an entry in a queue.
 */
public class QueueEntryPointer {
  protected final byte [] queueName;
  protected final long entryId;
  protected final long shardId;

  public QueueEntryPointer(final byte [] queueName, final long entryId,
      final long shardId) {
    this.queueName = queueName;
    this.entryId = entryId;
    this.shardId = shardId;
  }

  public byte [] getQueueName() {
    return this.queueName;
  }

  public long getEntryId() {
    return this.entryId;
  }

  public long getShardId() {
    return this.shardId;
  }

  @Override
  public boolean equals(Object o) {
    return this.entryId == ((QueueEntryPointer)o).entryId &&
        this.shardId == ((QueueEntryPointer)o).shardId;
  }

  @Override
  public int hashCode() {
    return Bytes.hashCode(
        Bytes.toBytes(entryId)) ^ Bytes.hashCode(Bytes.toBytes(shardId));
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("queueName", this.queueName)
        .add("entryId", this.entryId)
        .add("shardId", this.shardId)
        .toString();
  }

  public HBQEntryPointer toHBQ() {
    return new HBQEntryPointer(entryId, shardId);
  }
}
