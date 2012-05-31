package com.continuuity.data.operation.ttqueue.internal;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.operation.ttqueue.QueueEntryPointer;

/**
 * A pointer to an entry in the queue.
 */
public class EntryPointer extends QueueEntryPointer {
  
  public EntryPointer(final long entryId, final long shardId) {
    super(entryId, shardId);
  }
  
  public byte [] getBytes() {
    return Bytes.add(Bytes.toBytes(super.getEntryId()),
        Bytes.toBytes(super.getShardId()));
  }
  
  public static EntryPointer fromBytes(byte [] bytes) {
    return new EntryPointer(Bytes.toLong(bytes, 0), Bytes.toLong(bytes, 8));
  }
}
