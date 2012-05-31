package com.continuuity.data.operation.ttqueue;

import com.google.common.base.Objects;

/**
 * A single consumer from a single group.
 */
public class QueueConsumer {

  private final int instanceId;
  private final int groupId;
  private final int groupSize;

  /**
   * @param instanceId id of this consumer instance (starts at 0)
   * @param groupId id of this consumer group (doesn't matter)
   * @param groupSize number of consumer instances in this consumer group
   */
  public QueueConsumer(int instanceId, int groupId, int groupSize) {
    this.instanceId = instanceId;
    this.groupId = groupId;
    this.groupSize = groupSize;
  }

  public int getInstanceId() {
    return this.instanceId;
  }

  public int getGroupId() {
    return this.groupId;
  }

  public int getGroupSize() {
    return this.groupSize;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("instanceidd", this.instanceId)
        .add("groupid", this.groupId)
        .add("groupsize", this.groupSize)
        .toString();
  }
}
