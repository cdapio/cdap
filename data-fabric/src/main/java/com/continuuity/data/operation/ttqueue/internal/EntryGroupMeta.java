package com.continuuity.data.operation.ttqueue.internal;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Objects;

/**
 * Meta data for a group about a queue entry.
 */
public class EntryGroupMeta {

  private final EntryGroupState state;
  private final long timestamp;
  private final int instanceId;
  
  public EntryGroupMeta(final EntryGroupState state, final long timestamp,
      final int instanceId) {
    this.state = state;
    this.timestamp = timestamp;
    this.instanceId = instanceId;
  }

  public EntryGroupState getState() {
    return state;
  }

  public boolean isAvailable() {
    return state == EntryGroupState.AVAILABLE;
  }
  
  public boolean isAckedOrSemiAcked() {
    return state == EntryGroupState.SEMI_ACKED ||
        state == EntryGroupState.ACKED;
  }

  public boolean isSemiAcked() {
    return state == EntryGroupState.SEMI_ACKED;
  }
  
  public boolean isAcked() {
    return state == EntryGroupState.ACKED;
  }
  
  public boolean isDequeued() {
    return state == EntryGroupState.DEQUEUED;
  }
  
  public long getTimestamp() {
    return this.timestamp;
  }
  
  public int getInstanceId() {
    return this.instanceId;
  }
  
  public byte [] getBytes() {
    return Bytes.add(this.state.getBytes(), Bytes.toBytes(timestamp),
        Bytes.toBytes(instanceId));
  }
  
  public static EntryGroupMeta fromBytes(byte [] bytes) {
    return new EntryGroupMeta(EntryGroupState.fromBytes(new byte[] {bytes[0]}),
        Bytes.toLong(bytes, 1), Bytes.toInt(bytes, 9));
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("state", this.state)
        .add("timestamp", this.timestamp)
        .add("instanceId", this.instanceId)
        .toString();
  }
  
  public static enum EntryGroupState {
    AVAILABLE, SEMI_ACKED, ACKED, DEQUEUED;
    
    private static final byte [] AVAILABLE_BYTES = new byte [] { 0 };
    private static final byte [] SEMI_ACKED_BYTES = new byte [] { 1 };
    private static final byte [] ACKED_BYTES = new byte [] { 2 };
    private static final byte [] DEQUEUED_BYTES = new byte [] { 3 };
    
    public byte [] getBytes() {
      switch (this) {
        case AVAILABLE: return AVAILABLE_BYTES;
        case SEMI_ACKED:return SEMI_ACKED_BYTES;
        case ACKED:     return ACKED_BYTES;
        case DEQUEUED:  return DEQUEUED_BYTES;
      }
      return null;
    }
    
    public static EntryGroupState fromBytes(byte [] bytes) {
      if (bytes.length == 1) {
        if (bytes[0] == AVAILABLE_BYTES[0]) return AVAILABLE;
        if (bytes[0] == SEMI_ACKED_BYTES[0]) return SEMI_ACKED;
        if (bytes[0] == ACKED_BYTES[0]) return ACKED;
        if (bytes[0] == DEQUEUED_BYTES[0]) return DEQUEUED;
      }
      throw new RuntimeException("Invalid deserialization of EntryGroupState");
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("state", this.name())
          .toString();
    }
  }
}
