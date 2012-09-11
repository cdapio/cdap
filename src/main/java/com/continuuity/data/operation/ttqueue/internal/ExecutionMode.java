package com.continuuity.data.operation.ttqueue.internal;

import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.google.common.base.Objects;

/**
 * The execution mode of a {@link com.continuuity.data.operation.ttqueue.QueueConsumer}s within a group.
 */
public enum ExecutionMode {
  SINGLE_ENTRY,
  MULTI_ENTRY;
  
  private static final byte [] SINGLE_BYTES = new byte [] { 0 };
  private static final byte [] MULTI_BYTES = new byte [] { 1 };
  
  public byte [] getBytes() {
    return this == SINGLE_ENTRY ? SINGLE_BYTES : MULTI_BYTES;
  }
  
  public static ExecutionMode fromBytes(byte [] bytes) {
    if (bytes.length == 1) {
      if (bytes[0] == SINGLE_BYTES[0]) return SINGLE_ENTRY;
      if (bytes[0] == MULTI_BYTES[0]) return MULTI_ENTRY;
    }
    throw new RuntimeException("Invalid deserialization of ExecutionMode");
  }

  public static ExecutionMode fromQueueConfig(QueueConfig config) {
    return config.isSingleEntry() ? SINGLE_ENTRY : MULTI_ENTRY;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("mode", this.name())
        .toString();
  }

  public static ExecutionMode fromHBQ(
      com.continuuity.hbase.ttqueue.internal.ExecutionMode mode) {
    if (mode ==
        com.continuuity.hbase.ttqueue.internal.ExecutionMode.SINGLE_ENTRY) {
      return ExecutionMode.SINGLE_ENTRY;
    }
    return ExecutionMode.MULTI_ENTRY;
  }
}