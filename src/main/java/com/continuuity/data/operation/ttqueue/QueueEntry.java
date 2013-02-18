package com.continuuity.data.operation.ttqueue;

import com.google.common.base.Objects;

public class QueueEntry {
  private final byte[] header;
  private final byte[] data;

  public QueueEntry(byte[] header, byte[] data) {
    this.header = header;
    this.data = data;
  }

  public QueueEntry(byte[] data) {
    this(null, data);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("data", this.data)
        .add("header", this.header)
        .toString();
  }
}