package com.continuuity.data.stream;

import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;

/**
 * Consumer of Stream - currently Streams are tied to queue implementation. Will genericize and implement from
 * StreamConsumer interface when we have both HDFS and queue based stream implementation
 */
public class StreamQueueConsumer extends QueueConsumer {

  public StreamQueueConsumer(int instanceId, long groupId, int groupSize, QueueConfig config) {
    super(instanceId, groupId, groupSize, config);
  }

  public StreamQueueConsumer(int instanceId, long groupId, int groupSize, String groupName, QueueConfig config) {
    super(instanceId, groupId, groupSize, groupName, config);
  }

  public StreamQueueConsumer(int instanceId, long groupId, int groupSize, String groupName, String partitioningKey,
                             QueueConfig config) {
    super(instanceId, groupId, groupSize, groupName, partitioningKey, config);
  }
  public static StreamQueueConsumer fromQueueConsumer(QueueConsumer consumer) {
    return new StreamQueueConsumer(consumer.getInstanceId(), consumer.getGroupId(), consumer.getGroupSize(),
                                   consumer.getGroupName(), consumer.getPartitioningKey(), consumer.getQueueConfig());
  }

}
