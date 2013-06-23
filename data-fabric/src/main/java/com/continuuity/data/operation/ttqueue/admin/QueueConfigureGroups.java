package com.continuuity.data.operation.ttqueue.admin;

import com.continuuity.api.common.Bytes;
import com.continuuity.data.operation.Operation;
import com.google.common.base.Objects;

import java.util.List;

/**
 * Operation to configure the consumer groups for a queue.
 * This needs to be called with the list of consumer groups to configure.
 * Any other existing groups if any will be removed.
 */
public class QueueConfigureGroups extends Operation {
  private final byte [] queueName;
  private final List<Long> groupIds;

  /**
   * Create a QueueConfigureGroups object.
   * @param queueName queue name of the queue that needs to be configured.
   * @param groupIds list of groups that will be configured
   */
  public QueueConfigureGroups(byte[] queueName, List<Long> groupIds) {
    this.queueName = queueName;
    this.groupIds = groupIds;
  }

  public byte[] getQueueName() {
    return queueName;
  }

  public List<Long> getGroupIds() {
    return groupIds;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("queueName", Bytes.toString(queueName))
      .add("groupIds", groupIds)
      .toString();
  }
}
