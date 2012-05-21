package com.continuuity.data.operation.queue;

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
    return consumerId;
  }

  public int getGroupId() {
    return groupId;
  }

  public int getGroupSize() {
    return groupSize;
  }
  
  public boolean isSync() {
    return sync;
  }
  
  public boolean isAsync() {
    return !sync;
  }

  public boolean isInDrainMode() {
    return drain;
  }
  
  public void setDrainMode(boolean drain) {
    this.drain = drain;
  }

  @Override
  public String toString() {
    return "QueueConsumer consumerId=" + consumerId + ", groupId=" + groupId +
        ", groupSize=" + groupSize;
  }


}
