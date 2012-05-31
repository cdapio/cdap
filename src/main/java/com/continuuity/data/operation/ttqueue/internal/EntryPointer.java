package com.continuuity.data.operation.ttqueue.internal;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * A pointer to an entry in the queue.
 */
public class EntryPointer {

  private final long entryId;
  private final long shardId;
  
  public EntryPointer(final long entryId, final long shardId) {
    this.entryId = entryId;
    this.shardId = shardId;
  }

  public long getEntryid() {
    return this.entryId;
  }
  
  public long getShardId() {
    return this.shardId;
  }
  
  public byte [] getBytes() {
    return Bytes.add(Bytes.toBytes(entryId), Bytes.toBytes(shardId));
  }
  
  public static EntryPointer fromBytes(byte [] bytes) {
    return new EntryPointer(Bytes.toLong(bytes, 0), Bytes.toLong(bytes, 8));
  }
}
