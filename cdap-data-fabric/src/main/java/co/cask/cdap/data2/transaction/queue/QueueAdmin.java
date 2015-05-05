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

import java.util.Properties;
import javax.annotation.Nullable;

/**
 *
 */
public interface QueueAdmin {

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
   * Clears all queues for a flow, for example if the flow is upgraded and old.
   * todo: make this independent of the concept of a flow
   */
  void clearAllForFlow(String namespaceId, String app, String flow) throws Exception;

  /**
   * Returns a {@link QueueConfigurer} for configuring the queue.
   */
  QueueConfigurer getQueueConfigurer(QueueName queueName) throws Exception;

  /**
   * Performs upgrade action for all queues.
   */
  void upgrade() throws Exception;

  /**
   * @param queueName Name of the queue
   * @return true if queue with given name exists, otherwise false
   * @throws Exception if check fails
   */
  boolean exists(QueueName queueName) throws Exception;

  /**
   * Creates queue if doesn't exist. If queue exists does nothing.
   * @param queueName Name of the queue
   * @throws Exception if creation fails
   */
  void create(QueueName queueName) throws Exception;

  /**
   * Creates queue if doesn't exist. If queue exists does nothing.
   * @param queueName Name of the queue
   * @param props additional properties
   * @throws Exception if creation fails
   */
  void create(QueueName queueName, @Nullable Properties props) throws Exception;

  /**
   * Wipes out queue data.
   * @param queueName Name of the queue
   * @throws Exception if cleanup fails
   */
  void truncate(QueueName queueName) throws Exception;
}
