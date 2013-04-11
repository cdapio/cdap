package com.continuuity.data.operation.ttqueue;

import com.google.common.base.Objects;

/**
 *
 */
public class StatefulQueueConsumer extends QueueConsumer {
  private QueueState queueState = null;
  private final boolean canEvict;

  public StatefulQueueConsumer(int instanceId, long groupId, int groupSize, QueueConfig config, boolean canEvict) {
    super(instanceId, groupId, groupSize, config);
    this.canEvict = canEvict;
  }

  public StatefulQueueConsumer(int instanceId, long groupId, int groupSize, String groupName, QueueConfig config,
                               boolean canEvict) {
    super(instanceId, groupId, groupSize, groupName, config);
    this.canEvict = canEvict;
  }

  public StatefulQueueConsumer(int instanceId, long groupId, int groupSize, String groupName, String partitioningKey,
                               QueueConfig config, boolean canEvict) {
    super(instanceId, groupId, groupSize, groupName, partitioningKey, config);
    this.canEvict = canEvict;
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
  public boolean canEvict() {
    return canEvict;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("super", super.toString())
      .add("queueState", queueState)
      .add("canEvict", canEvict)
      .toString();
  }
}
