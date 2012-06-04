package com.continuuity.data.operation.ttqueue;

import com.google.common.base.Objects;

/**
 * Result from a {@link QueueDequeue} operation.
 */
public class DequeueResult {

  private final DequeueStatus status;
  private final QueueEntryPointer pointer;
  private final byte [] value;
  private final String msg;

  public DequeueResult(final DequeueStatus status) {
    this(status, null, null, null);
  }

  public DequeueResult(final DequeueStatus status, String msg) {
    this(status, msg, null, null);
  }
  
  public DequeueResult(final DequeueStatus status,
      final QueueEntryPointer pointer, final byte [] value) {
    this(status, null, pointer, value);
  }

  private DequeueResult(final DequeueStatus status, final String msg,
      final QueueEntryPointer pointer, final byte [] value) {
    this.status = status;
    this.msg = msg;
    this.pointer = pointer;
    this.value = value;
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

  public QueueEntryPointer getEntryPointer() {
    return this.pointer;
  }
  
  public byte [] getValue() {
    return this.value;
  }

  public static enum DequeueStatus {
    SUCCESS, EMPTY, FAILURE, RETRY;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("status", this.status)
        .add("entryPointer", this.pointer)
        .add("value.length", this.value != null ? this.value.length : 0)
        .add("msg", this.msg)
        .toString();
  }

  public String getMsg() {
    return msg;
  }
}
