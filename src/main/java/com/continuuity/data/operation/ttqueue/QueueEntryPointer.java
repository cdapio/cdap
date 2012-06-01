package com.continuuity.data.operation.ttqueue;

import com.google.common.base.Objects;

/**
 * A pointer which completely addresses an entry in a queue.
 */
public class QueueEntryPointer {
  protected final long entryId;
  protected final long shardId;
  
  public QueueEntryPointer(final long entryId, final long shardId) {
    this.entryId = entryId;
    this.shardId = shardId;
  }

  public long getEntryId() {
    return this.entryId;
  }
  
  public long getShardId() {
    return this.shardId;
  }

  @Override
  public boolean equals(Object o) {
    return entryId == ((QueueEntryPointer)o).entryId &&
        shardId == ((QueueEntryPointer)o).shardId;
  }
  
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("entryId", this.entryId)
        .add("shardId", this.shardId)
        .toString();
  }
}
