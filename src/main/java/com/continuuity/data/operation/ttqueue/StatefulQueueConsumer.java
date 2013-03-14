package com.continuuity.data.operation.ttqueue;

/**
 *
 */
public class StatefulQueueConsumer extends QueueConsumer {
  private QueueState queueState = null;

  public StatefulQueueConsumer(int instanceId, long groupId, int groupSize) {
    super(instanceId, groupId, groupSize);
  }

  public StatefulQueueConsumer(int instanceId, long groupId, int groupSize, String groupName) {
    super(instanceId, groupId, groupSize, groupName);
  }

  public StatefulQueueConsumer(int instanceId, long groupId, int groupSize, QueueConfig config) {
    super(instanceId, groupId, groupSize, config);
  }

  public StatefulQueueConsumer(int instanceId, long groupId, int groupSize, String groupName, QueueConfig config) {
    super(instanceId, groupId, groupSize, groupName, config);
  }

  public StatefulQueueConsumer(int instanceId, long groupId, int groupSize, String groupName, String partitioningKey, QueueConfig config) {
    super(instanceId, groupId, groupSize, groupName, partitioningKey, config);
  }

  @Override
  public boolean isStateful() {
    return true;
  }

  @Override
  public QueueState getQueueState() {
    return queueState;
  }

  @Override
  public void setQueueState(QueueState queueState) {
    this.queueState = queueState;
  }
}
