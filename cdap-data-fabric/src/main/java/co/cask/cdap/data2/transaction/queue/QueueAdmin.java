/*
 * Copyright © 2014-2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.transaction.queue;

import co.cask.cdap.common.queue.QueueName;

import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 *
 */
public interface QueueAdmin {

  /**
   * Deletes all entries for all queues.
   */
  void dropAll() throws Exception;

  /**
   * Deletes all queues in a namespace
   * @param namespaceId the namespace to delete flows in
   */
  void dropAllInNamespace(String namespaceId) throws Exception;

  /**
   * Deletes all queues for a flow, for example if the flow is deleted.
   * todo: make this independent of the concept of a flow
   */
  void dropAllForFlow(String namespaceId, String app, String flow) throws Exception;

  /**
   * Clears all queues for a flow, for example if the flow is upgraded and old .
   * todo: make this independent of the concept of a flow
   */
  void clearAllForFlow(String namespaceId, String app, String flow) throws Exception;

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

  ////////////////////////
  /**
   * @param name entity name
   * @return true if entity with given name exists, otherwise false
   * @throws Exception if check fails
   */
  boolean exists(String name) throws Exception;

  /**
   * Creates entity if doesn't exist. If entity exists does nothing.
   * @param name name of the entity to create
   * @throws Exception if creation fails
   */
  void create(String name) throws Exception;

  /**
   * Creates entity if doesn't exist. If entity exists does nothing.
   * @param name name of the entity to create
   * @param props additional properties
   * @throws Exception if creation fails
   */
  void create(String name, @Nullable Properties props) throws Exception;

  /**
   * Wipes out entity data.
   * @param name entity name
   * @throws Exception if cleanup fails
   */
  void truncate(String name) throws Exception;

  /**
   * Deletes entity from the system completely.
   * @param name entity name
   * @throws Exception if deletion fails
   */
  void drop(String name) throws Exception;

  /**
   * Performs update of entity.
   *
   * @param name Name of the entity to update
   * @throws Exception if update fails
   */
  void upgrade(String name, Properties properties) throws Exception;
}
