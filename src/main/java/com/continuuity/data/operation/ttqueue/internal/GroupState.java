package com.continuuity.data.operation.ttqueue.internal;

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


}
