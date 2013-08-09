package com.continuuity.data.operation.ttqueue;

import com.google.common.base.Objects;

/**
 * A single consumer from a single group.
 */
public class QueueConsumer {
  private final int instanceId;
  private final long groupId;
  private final int groupSize;
  private final QueueConfig config;
  private final String groupName; // may be null
  private final String partitioningKey; // may be null or empty
  private StateType stateType = StateType.UNINITIALIZED;

  /**
   * Defines queue consumer states.
   */
  public enum StateType {
    UNINITIALIZED, // Consumer does not have its state, it could be due to consumer's first call or consumer crash
    INITIALIZED,   // Consumer has its state available
    NOT_FOUND      // Consumer has its state but the state is not available due to eviction from cache
  }

  /**
   * @param instanceId id of this consumer instance (starts at 0)
   * @param groupId id of this consumer group (doesn't matter)
   * @param groupSize number of consumer instances in this consumer group
   */
  public QueueConsumer(int instanceId, long groupId, int groupSize, QueueConfig config) {
    this(instanceId, groupId, groupSize, null, null, config);
  }
  /**
   * @param instanceId id of this consumer instance (starts at 0)
   * @param groupId id of this consumer group (doesn't matter)
   * @param groupSize number of consumer instances in this consumer group
   * @param groupName the name of the consumer group
   * @param config the queue configuration of the consumer group
   */
  public QueueConsumer(int instanceId, long groupId, int groupSize, String groupName, QueueConfig config) {
    this(instanceId, groupId, groupSize, groupName, null, config);
  }

  /**
   * @param instanceId id of this consumer instance (starts at 0)
   * @param groupId id of this consumer group (doesn't matter)
   * @param groupSize number of consumer instances in this consumer group
   * @param groupName the name of the consumer group
   * @param partitioningKey the partitioning key of the consumer group
   * @param config the queue configuration of the consumer group
   */
  public QueueConsumer(int instanceId, long groupId, int groupSize, String groupName, String partitioningKey,
                       QueueConfig config) {
    if (instanceId >= groupSize) {
      throw new IllegalArgumentException(String.format(
        "instanceId should be between 0..groupSize. Given instanceId is %d, groupSize is %d", instanceId, groupSize));
    }
    this.instanceId = instanceId;
    this.groupId = groupId;
    this.groupSize = groupSize;
    this.groupName = groupName;
    this.partitioningKey = partitioningKey;
    this.config = config;
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

  public String getGroupName() {
    return this.groupName;
  }

  public String getPartitioningKey() {
    return this.partitioningKey;
  }

  public QueueConfig getQueueConfig() {
    return this.config;
  }

  public boolean isStateful() {
    return false;
  }

  public QueueState getQueueState() {
    return null;
  }

  public void setQueueState(QueueState queueState) {
    // Nothing to do
  }

  public StateType getStateType() {
    return stateType;
  }

  public void setStateType(StateType stateType) {
    this.stateType = stateType;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("instanceidd", this.instanceId)
        .add("groupid", this.groupId)
        .add("groupsize", this.groupSize)
        .add("config", this.config)
        .add("name", this.groupName)
        .add("partitioningKey", this.partitioningKey)
        .add("stateType", this.stateType)
        .toString();
  }
}
