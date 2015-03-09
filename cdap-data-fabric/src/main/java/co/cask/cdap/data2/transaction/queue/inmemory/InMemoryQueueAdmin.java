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

package co.cask.cdap.data2.transaction.queue.inmemory;

import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.Map;
import java.util.Properties;

/**
 * admin for queues in memory.
 */
@Singleton
public class InMemoryQueueAdmin implements QueueAdmin {
  protected final InMemoryQueueService queueService;

  @Inject
  public InMemoryQueueAdmin(InMemoryQueueService queueService) {
    this.queueService = queueService;
  }

  @Override
  public boolean exists(QueueName queueName) throws Exception {
    // no special actions needed to create it
    return queueService.exists(queueName);
  }

  @Override
  public void create(QueueName queueName) throws Exception {
    queueService.getQueue(queueName);
  }

  @Override
  public void create(QueueName queueName, @SuppressWarnings("unused") Properties props) throws Exception {
    create(queueName);
  }

  @Override
  public void truncate(QueueName queueName) throws Exception {
    queueService.truncate(queueName);
  }

  @Override
  public void clearAllForFlow(String namespaceId, String app, String flow) throws Exception {
    queueService.truncateAllWithPrefix(QueueName.prefixForFlow(namespaceId, app, flow));
  }

  // Only used by InMemoryStreadmAdmin
  void drop(QueueName queueName) throws Exception {
    queueService.drop(queueName);
  }

  @Override
  public void dropAllInNamespace(String namespaceId) throws Exception {
    queueService.resetQueuesWithPrefix(QueueName.prefixForNamespacedQueue(namespaceId));
  }

  @Override
  public void dropAllForFlow(String namespaceId, String app, String flow) throws Exception {
    queueService.resetQueuesWithPrefix(QueueName.prefixForFlow(namespaceId, app, flow));
  }

  @Override
  public void configureInstances(QueueName queueName, long groupId, int instances) {
    // No-op for InMemoryQueueAdmin
    // Potentially refactor QueueClientFactory to have better way to handle instances and group info.
  }

  @Override
  public void configureGroups(QueueName queueName, Map<Long, Integer> groupInfo) {
    // No-op for InMemoryQueueAdmin
    // Potentially refactor QueueClientFactory to have better way to handle instances and group info.
  }

  @Override
  public void upgrade() throws Exception {
    // No-op
  }
}
