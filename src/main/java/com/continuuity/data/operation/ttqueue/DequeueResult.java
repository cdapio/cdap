package com.continuuity.data.operation.ttqueue;

import com.google.common.base.Objects;

/**
 * Result from a {@link QueueDequeue} operation.
 */
public class DequeueResult {

  private final DequeueStatus status;
  private final long entryId;
  private final byte [] value;
  private final String msg;

  public DequeueResult(final DequeueStatus status, String msg) {
    this.status = status;
    this.msg = msg;
    this.entryId = -1;
    this.value = null;
  }
  
  public DequeueResult(final DequeueStatus status, final long entryId,
      final byte [] value) {
    this.status = status;
    this.entryId = entryId;
    this.value = value;
    this.msg = null;
  }

  public boolean isSuccess() {
    return this.status == DequeueStatus.SUCCESS;
  }

  public boolean isFailure() {
    return this.status == DequeueStatus.FAILURE;
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

  public long getEntryId() {
    return this.entryId;
  }

  public static enum DequeueStatus {
    SUCCESS, EMPTY, RETRY, FAILURE;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("status", this.status)
        .add("entryId", this.entryId)
        .add("value.length", this.value.length)
        .toString();
  }

  public String getMsg() {
    return msg;
  }
}
