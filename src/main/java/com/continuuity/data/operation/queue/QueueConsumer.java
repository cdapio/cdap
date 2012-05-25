package com.continuuity.data.operation.queue;

import com.google.common.base.Objects;

public class QueueConsumer {

  private final int consumerId;
  private final int groupId;
  private final int groupSize;
  private final boolean sync;
  private boolean drain;

  /**
   * @param consumerId id of this consumer instance (starts at 0)
   * @param groupId id of this consumer group (doesn't matter)
   * @param groupSize number of consumer instances in this consumer group
   */
  public QueueConsumer(int consumerId, int groupId, int groupSize,
      boolean sync, boolean drain) {
    this.consumerId = consumerId;
    this.groupId = groupId;
    this.groupSize = groupSize;
    this.sync = sync;
  }

  public int getConsumerId() {
    return this.consumerId;
  }

  public int getGroupId() {
    return this.groupId;
  }

  public int getGroupSize() {
    return this.groupSize;
  }

  public boolean isSync() {
    return this.sync;
  }

  public boolean isAsync() {
    return !this.sync;
  }

  public boolean isInDrainMode() {
    return this.drain;
  }

  public void setDrainMode(boolean drain) {
    this.drain = drain;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("consumerid", this.consumerId)
        .add("groupid", this.groupId)
        .add("groupsize", this.groupSize)
        .add("syncmode", this.sync)
        .add("drainmode", this.drain)
        .toString();
  }
}
