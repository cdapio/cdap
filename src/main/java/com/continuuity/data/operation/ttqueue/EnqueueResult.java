package com.continuuity.data.operation.ttqueue;

import com.google.common.base.Objects;

/**
 * Result from an {@link QueueEnqueue} operation.
 */
public class EnqueueResult {

  private final EnqueueStatus status;
  private final QueueEntryPointer entryPointer;

  public EnqueueResult(final EnqueueStatus status,
      final QueueEntryPointer entryPointer) {
    this.status = status;
    this.entryPointer = entryPointer;
  }

  public boolean isSuccess() {
    return this.status == EnqueueStatus.SUCCESS;
  }

  public QueueEntryPointer getEntryPointer() {
    return this.entryPointer;
  }

  public static enum EnqueueStatus {
    SUCCESS;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("status", this.status)
        .add("entryPointer", this.entryPointer)
        .toString();
  }
}
