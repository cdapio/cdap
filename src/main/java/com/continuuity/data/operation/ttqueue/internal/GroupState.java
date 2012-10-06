package com.continuuity.data.operation.ttqueue.internal;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Objects;

public class GroupState {

  private final int groupSize;
  private final EntryPointer head;
  private final ExecutionMode mode;

  public GroupState(final int groupSize, final EntryPointer head,
      final ExecutionMode mode) {
    this.groupSize = groupSize;
    this.head = head;
    this.mode = mode;
  }

  public int getGroupSize() {
    return this.groupSize;
  }

  public EntryPointer getHead() {
    return this.head;
  }

  public ExecutionMode getMode() {
    return this.mode;
  }
  
  public byte [] getBytes() {
    return Bytes.add(
        Bytes.toBytes(groupSize), // 4 bytes
        head.getBytes(),          // 16 bytes
        mode.getBytes());         // 1 byte
  }
  
  public static GroupState fromBytes(byte [] bytes) {
    return new GroupState(Bytes.toInt(bytes),
        new EntryPointer(Bytes.toLong(bytes, 4), Bytes.toLong(bytes, 12)),
        ExecutionMode.fromBytes(Bytes.tail(bytes, 1)));
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GroupState)) return false;
    GroupState gs = (GroupState)o;
    if (gs.getGroupSize() != this.groupSize) return false;
    if (!gs.getHead().equals(this.head)) return false;
    if (gs.getMode() != this.mode) return false; 
    return true;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("groupSize", this.groupSize)
        .add("headPointer", this.head)
        .add("execMode", this.mode.name())
        .toString();
  }
}
