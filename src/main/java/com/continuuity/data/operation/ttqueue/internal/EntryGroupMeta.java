package com.continuuity.data.operation.ttqueue.internal;

import org.apache.hadoop.hbase.util.Bytes;

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

  public boolean isAvailabe() {
    return state == EntryGroupState.AVAILABLE;
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
  
  public long getInstanceId() {
    return this.instanceId;
  }
  
  public byte [] getBytes() {
    return Bytes.add(Bytes.toBytes(instanceId), Bytes.toBytes(timestamp),
        this.state.getBytes());
  }
  
  public static EntryGroupMeta fromBytes(byte [] bytes) {
    return new EntryGroupMeta(EntryGroupState.fromBytes(new byte[] {bytes[12]}),
        Bytes.toLong(bytes, 0), Bytes.toInt(bytes, 8));
  }
  
  public static enum EntryGroupState {
    AVAILABLE, ACKED, DEQUEUED;
    
    private static final byte [] AVAILABLE_BYTES = new byte [] { 0 };
    private static final byte [] ACKED_BYTES = new byte [] { 1 };
    private static final byte [] DEQUEUED_BYTES = new byte [] { 2 };
    
    public byte [] getBytes() {
      switch (this) {
        case AVAILABLE: return AVAILABLE_BYTES;
        case ACKED:     return ACKED_BYTES;
        case DEQUEUED:  return DEQUEUED_BYTES;
      }
      return null;
    }
    
    public static EntryGroupState fromBytes(byte [] bytes) {
      if (bytes.length == 1) {
        if (bytes[0] == AVAILABLE_BYTES[0]) return AVAILABLE;
        if (bytes[0] == ACKED_BYTES[0]) return ACKED;
        if (bytes[0] == DEQUEUED_BYTES[0]) return DEQUEUED;
      }
      throw new RuntimeException("Invalid deserialization of EntryGroupState");
    }
  }
}
