package com.continuuity.data.operation.ttqueue;

import com.google.common.base.Objects;

/**
 * Result from an {@link QueueEnqueue} operation.
 */
public class EnqueueResult {

  private final EnqueueStatus status;
  private final long entryId;

  public EnqueueResult(final EnqueueStatus status, final long entryId) {
    this.status = status;
    this.entryId = entryId;
  }

  public boolean isSuccess() {
    return this.status == EnqueueStatus.SUCCESS;
  }

  public long getEntryId() {
    return this.entryId;
  }

  public static enum EnqueueStatus {
    SUCCESS;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("status", this.status)
        .add("entryId", this.entryId)
        .toString();
  }
}
