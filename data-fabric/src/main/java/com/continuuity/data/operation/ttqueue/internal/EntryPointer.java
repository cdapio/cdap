package com.continuuity.data.operation.ttqueue.internal;

import com.continuuity.data.operation.ttqueue.QueueEntryPointer;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A pointer to an entry in the queue.
 */
public class EntryPointer extends QueueEntryPointer {

  public EntryPointer(final long entryId,
      final long shardId) {
    super(null, entryId, shardId);
  }

  public EntryPointer makeCopy() {
    return new EntryPointer(super.entryId, super.shardId);
  }

  public byte [] getBytes() {
    return Bytes.add(Bytes.toBytes(super.entryId),
        Bytes.toBytes(super.shardId));
  }

  public static EntryPointer fromBytes(byte [] bytes) {
    return new EntryPointer(Bytes.toLong(bytes, 0), Bytes.toLong(bytes, 8));
  }

  @Override
  public boolean equals(Object o) {
    return entryId == ((EntryPointer) o).entryId &&
        shardId == ((EntryPointer) o).shardId;
  }
}
