package com.continuuity.data.operation.ttqueue;

import com.continuuity.hbase.ttqueue.HBQDequeueResult;
import com.continuuity.hbase.ttqueue.HBQDequeueResult.HBQDequeueStatus;
import com.google.common.base.Objects;

/**
 * Result from a {@link QueueDequeue} operation.
 */
public class DequeueResult {

  private final DequeueStatus status;
  private final QueueEntryPointer pointer;
  private final QueueEntry entry;

  public DequeueResult(final DequeueStatus status) {
    this(status, null, null);
  }

  public DequeueResult(final DequeueStatus status,
      final QueueEntryPointer pointer, final QueueEntry entry) {
    this.status = status;
    this.pointer = pointer;
    this.entry = entry;
  }
  
  public DequeueResult(final byte [] queueName,
      final HBQDequeueResult dequeueResult) {
    if (dequeueResult.getStatus() == HBQDequeueStatus.EMPTY) {
      this.status = DequeueStatus.EMPTY;
      this.pointer = null;
//      this.value = null;
      //TODO: is this correct?
      this.entry = null;
    } else if (dequeueResult.getStatus() == HBQDequeueStatus.SUCCESS) {
      this.status = DequeueStatus.SUCCESS;
      this.pointer = new QueueEntryPointer(queueName,
          dequeueResult.getEntryPointer().getEntryId(),
          dequeueResult.getEntryPointer().getShardId());
      this.entry = new QueueEntryImpl(dequeueResult.getData());
    } else {
      throw new RuntimeException("Invalid state: " + dequeueResult.toString());
    }
  }

  public boolean isSuccess() {
    return this.status == DequeueStatus.SUCCESS;
  }

  public boolean isEmpty() {
    return this.status == DequeueStatus.EMPTY;
  }

  public boolean shouldRetry() {
    return this.status == DequeueStatus.RETRY;
  }

  public DequeueStatus getStatus() {
    return this.status;
  }

  public QueueEntryPointer getEntryPointer() {
    return this.pointer;
  }
  
  public QueueEntry getEntry() {
    return this.entry;
  }

  public static enum DequeueStatus {
    SUCCESS, EMPTY, RETRY;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("status", this.status)
        .add("entryPointer", this.pointer)
        .add("entry", this.entry)
        .toString();
  }
  @Deprecated
  public byte[] getValue() {
    return entry.getData();
  }
}
