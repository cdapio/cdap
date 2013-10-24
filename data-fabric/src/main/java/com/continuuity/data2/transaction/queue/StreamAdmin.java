package com.continuuity.data2.transaction.queue;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.dataset.api.DataSetManager;

import java.util.Map;

/**
 *
 */
public interface StreamAdmin extends DataSetManager {

  /**
   * Deletes all entries for all queues.
   */
  void dropAll() throws Exception;

  /**
   * Sets the number of consumer instances for the given consumer group in a queue.
   * @param queueName Name of the queue.
   * @param groupId The consumer group to alter.
   * @param instances Number of instances.
   */
  void configureInstances(QueueName queueName, long groupId, int instances) throws Exception;


  /**
   * Sets the consumer groups information for the given queue.
   * @param queueName Name of the queue.
   * @param groupInfo A map from groupId to number of instances of each group.
   */
  void configureGroups(QueueName queueName, Map<Long, Integer> groupInfo) throws Exception;
}
