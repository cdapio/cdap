package com.continuuity.data.operation.ttqueue;

import com.continuuity.hbase.ttqueue.HBQConsumer;
import com.google.common.base.Objects;

/**
 * A single consumer from a single group.
 */
public class QueueConsumer {

  private final int instanceId;
  private final long groupId;
  private final int groupSize;

  /**
   * @param instanceId id of this consumer instance (starts at 0)
   * @param groupId id of this consumer group (doesn't matter)
   * @param groupSize number of consumer instances in this consumer group
   */
  public QueueConsumer(int instanceId, long groupId, int groupSize) {
    this.instanceId = instanceId;
    this.groupId = groupId;
    this.groupSize = groupSize;
  }

  public int getInstanceId() {
    return this.instanceId;
  }

  public long getGroupId() {
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

  public HBQConsumer toHBQ() {
    return new HBQConsumer(instanceId, groupId, groupSize);
  }
}
