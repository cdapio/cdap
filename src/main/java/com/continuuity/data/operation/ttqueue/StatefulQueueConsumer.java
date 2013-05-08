package com.continuuity.data.operation.ttqueue;

import com.google.common.base.Objects;

/**
 *
 */
public class StatefulQueueConsumer extends QueueConsumer {
  // This object will be used in multi-threaded context. Hence needs to be thread safe.
  private volatile QueueState queueState = null;

  public StatefulQueueConsumer(int instanceId, long groupId, int groupSize, QueueConfig config) {
    super(instanceId, groupId, groupSize, config);
  }

  public StatefulQueueConsumer(int instanceId, long groupId, int groupSize, String groupName, QueueConfig config) {
    super(instanceId, groupId, groupSize, groupName, config);
  }

  public StatefulQueueConsumer(int instanceId, long groupId, int groupSize, String groupName, String partitioningKey,
                               QueueConfig config) {
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

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("super", super.toString())
      .add("queueState", queueState)
      .toString();
  }
}
