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
  private final QueueConfig config;
  private final String groupName; // may be null
  private final String partitioningKey; // may be null or empty

  /**
   * @param instanceId id of this consumer instance (starts at 0)
   * @param groupId id of this consumer group (doesn't matter)
   * @param groupSize number of consumer instances in this consumer group
   * @deprecated
   */
  public QueueConsumer(int instanceId, long groupId, int groupSize) {
    this(instanceId, groupId, groupSize, null, null, null);
  }
  /**
   * @param instanceId id of this consumer instance (starts at 0)
   * @param groupId id of this consumer group (doesn't matter)
   * @param groupSize number of consumer instances in this consumer group
   * @param groupName the name of the consumer group
   * @deprecated
   */
  public QueueConsumer(int instanceId, long groupId, int groupSize, String groupName) {
    this(instanceId, groupId, groupSize, groupName, null, null);
  }
  /**
   * @param instanceId id of this consumer instance (starts at 0)
   * @param groupId id of this consumer group (doesn't matter)
   * @param groupSize number of consumer instances in this consumer group
   * @deprecated
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
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("instanceidd", this.instanceId)
        .add("groupid", this.groupId)
        .add("groupsize", this.groupSize)
        .add("name", this.groupName)
        .toString();
  }

  public HBQConsumer toHBQ() {
    return new HBQConsumer(instanceId, groupId, groupSize);
  }
}
