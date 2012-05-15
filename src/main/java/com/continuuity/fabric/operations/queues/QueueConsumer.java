package com.continuuity.fabric.operations.queues;

public class QueueConsumer {

  private final int consumerId;
  private final int groupId;
  private final int groupSize;

  /**
   * @param consumerId id of this consumer instance (starts at 0)
   * @param groupId id of this consumer group (doesn't matter)
   * @param groupSize number of consumer instances in this consumer group
   */
  public QueueConsumer(int consumerId, int groupId, int groupSize) {
    this.consumerId = consumerId;
    this.groupId = groupId;
    this.groupSize = groupSize;
  }

  public int getConsumerId() {
    return consumerId;
  }

  public int getGroupId() {
    return groupId;
  }

  public int getGroupSize() {
    return groupSize;
  }


}
