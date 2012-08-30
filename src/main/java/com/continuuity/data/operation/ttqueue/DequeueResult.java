package com.continuuity.data.operation.ttqueue;

import com.google.common.base.Objects;

/**
 * Result from a {@link QueueDequeue} operation.
 */
public class DequeueResult {

  private final QueueEntryPointer pointer;
  private final byte [] value;

  public DequeueResult(final QueueEntryPointer pointer, final byte [] value) {
    this.pointer = pointer;
    this.value = value;
  }
  
  public QueueEntryPointer getEntryPointer() {
    return this.pointer;
  }
  
  public byte [] getValue() {
    return this.value;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("entryPointer", this.pointer)
        .add("value.length", this.value != null ? this.value.length : 0)
        .toString();
  }
}
