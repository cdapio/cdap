package com.continuuity.data2.transaction.queue;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.dataset.api.DataSetManager;

import java.util.Map;

/**
 *
 */
public interface QueueAdmin extends DataSetManager {

  /**
   * Deletes all entries for all queues.
   */
  void dropAll() throws Exception;

  /**
   * Deletes all queues for a flow, for example if the flow is deleted.
   * todo: make this independent of the concept of a flow
   */
  void dropAllForFlow(String app, String flow) throws Exception;

  /**
   * Clears all queues for a flow, for example if the flow is upgraded and old .
   * todo: make this independent of the concept of a flow
   */
  void clearAllForFlow(String app, String flow) throws Exception;

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

  /**
   * Performs upgrade action for all queues.
   */
  void upgrade() throws Exception;
}
