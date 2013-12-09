package com.continuuity.gateway.handlers.stream;

import com.continuuity.common.queue.QueueName;

/**
 * Key for Queue Consumer cache.
 */
final class ConsumerKey {
  private final QueueName queueName;
  private final long groupId;

  ConsumerKey(QueueName queueName, long groupId) {
    this.queueName = queueName;
    this.groupId = groupId;
  }

  public QueueName getQueueName() {
    return queueName;
  }

  public long getGroupId() {
    return groupId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ConsumerKey that = (ConsumerKey) o;

    return groupId == that.groupId && queueName.equals(that.queueName);

  }

  @Override
  public int hashCode() {
    int result = queueName.hashCode();
    result = 31 * result + (int) (groupId ^ (groupId >>> 32));
    return result;
  }
}
