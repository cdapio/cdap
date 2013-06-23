package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.hbase.ttqueue.HBQEntryPointer;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * A pointer which completely addresses an entry in a queue.
 */
public class QueueEntryPointer {
  protected final byte [] queueName;
  protected final long entryId;
  protected final long shardId;
  protected final int tries;

  public QueueEntryPointer(final byte [] queueName, final long entryId,
      final long shardId) {
    this.queueName = queueName;
    this.entryId = entryId;
    this.shardId = shardId;
    this.tries = 0;
  }

  public QueueEntryPointer(byte[] queueName, long entryId) {
    this.queueName = queueName;
    this.entryId = entryId;
    this.shardId = -1; // Single shard
    this.tries = 0;
  }

  public QueueEntryPointer(byte[] queueName, long entryId, int tries) {
    this.queueName = queueName;
    this.entryId = entryId;
    this.shardId = -1;
    this.tries = tries;
  }

  public QueueEntryPointer(byte[] queueName, long entryId, long shardId, int tries) {
    this.queueName = queueName;
    this.entryId = entryId;
    this.shardId = shardId;
    this.tries = tries;
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

  public int getTries() {
    return tries;
  }

  @Override
  public boolean equals(Object o) {
    // tries doesn't affect object identity
    return this.entryId == ((QueueEntryPointer)o).entryId &&
        this.shardId == ((QueueEntryPointer)o).shardId;
  }

  @Override
  public int hashCode() {
    // tries doesn't affect object identity
    return Bytes.hashCode(Bytes.toBytes(entryId)) ^ Bytes.hashCode(Bytes.toBytes(shardId));
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("queueName", this.queueName)
        .add("entryId", this.entryId)
        .add("shardId", this.shardId)
        .add("tries", this.tries)
        .toString();
  }

  public HBQEntryPointer toHBQ() {
    return new HBQEntryPointer(entryId, shardId);
  }

  /**
   * Serialize QueueEntry into byte array
   * @return serialized byte array containing entryId, shardId and queueName
   */
  public byte [] getBytes() {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    BinaryEncoder binaryEncoder = new BinaryEncoder(bos);
    try {
      binaryEncoder.writeLong(this.entryId);
      binaryEncoder.writeLong(this.shardId);
      binaryEncoder.writeBytes(this.queueName);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return bos.toByteArray();
  }
}
