package com.continuuity.data.operation.ttqueue.internal;

import com.google.common.base.Objects;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Meta data about the current shard.
 */
public class ShardMeta {

  private final long shardId;
  private final long shardBytes;
  private final long shardEntries;

  public ShardMeta(final long shardId, final long shardBytes,
      final long shardEntries) {
    this.shardId = shardId;
    this.shardBytes = shardBytes;
    this.shardEntries = shardEntries;
  }

  public long getShardId() {
    return this.shardId;
  }

  public long getShardBytes() {
    return this.shardBytes;
  }

  public long getShardEntries() {
    return this.shardEntries;
  }

  public byte [] getBytes() {
    return Bytes.add(Bytes.toBytes(this.shardId),
        Bytes.toBytes(this.shardBytes), Bytes.toBytes(this.shardEntries));
  }

  public static ShardMeta fromBytes(byte [] bytes) {
    return new ShardMeta(Bytes.toLong(bytes, 0), Bytes.toLong(bytes, 8),
        Bytes.toLong(bytes, 16));
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("shardId", this.shardId)
        .add("shardBytes", this.shardBytes)
        .add("shardEntries", this.shardEntries)
        .toString();
  }
}
