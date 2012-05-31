package com.continuuity.data.operation.ttqueue.internal;

/**
 * The execution mode of a {@link QueueConsumer}s within a group.
 */
public enum ExecutionMode {
  SYNC,
  ASYNC;
  
  private static final byte [] SYNC_BYTES = new byte [] { 0 };
  private static final byte [] ASYNC_BYTES = new byte [] { 1 };
  
  public byte [] getBytes() {
    return this == SYNC ? SYNC_BYTES : ASYNC_BYTES;
  }
  
  public static ExecutionMode fromBytes(byte [] bytes) {
    if (bytes.length == 1) {
      if (bytes[0] == SYNC_BYTES[0]) return SYNC;
      if (bytes[0] == ASYNC_BYTES[0]) return ASYNC;
    }
    throw new RuntimeException("Invalid deserialization of ExecutionMode");
  }
}