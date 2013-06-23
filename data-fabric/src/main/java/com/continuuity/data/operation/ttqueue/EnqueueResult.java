package com.continuuity.data.operation.ttqueue;

import com.google.common.base.Objects;

import java.util.Arrays;

/**
 * Result from an {@link QueueEnqueue} operation.
 */
public class EnqueueResult {

  private final EnqueueStatus status;
  private final QueueEntryPointer [] entryPointers;

  public EnqueueResult(final EnqueueStatus status,
                       final QueueEntryPointer entryPointer) {
    this.status = status;
    this.entryPointers = new QueueEntryPointer[] { entryPointer };
  }

  public EnqueueResult(final EnqueueStatus status,
                       final QueueEntryPointer [] entryPointers) {
    this.status = status;
    this.entryPointers = entryPointers;
  }

  public boolean isSuccess() {
    return this.status == EnqueueStatus.SUCCESS;
  }

  public EnqueueStatus getStatus() {
    return status;
  }

  public QueueEntryPointer [] getEntryPointers() {
    return this.entryPointers;
  }

  public QueueEntryPointer getEntryPointer() {
    return this.entryPointers[0];
  }

  public static enum EnqueueStatus {
    SUCCESS
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("status", this.status)
        .add("entryPointers", Arrays.toString(this.entryPointers))
        .toString();
  }
}
