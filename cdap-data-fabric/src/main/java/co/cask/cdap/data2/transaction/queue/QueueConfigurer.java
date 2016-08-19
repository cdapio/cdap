/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.data2.queue.ConsumerGroupConfig;
import org.apache.tephra.TransactionAware;

import java.io.Closeable;

/**
 * Provides methods to configure queue. Configuration needs to happen inside a transaction.
 */
public interface QueueConfigurer extends TransactionAware, Closeable {

  /**
   * Changes the number of consumer instances of the given consumer group. The consumer group configuration needs to be
   * existed already.
   *
   * @param groupId groupId of the consumer group
   * @param instances number of instances to change to
   * @throws Exception if failed to change number of instances.
   */
  void configureInstances(long groupId, int instances) throws Exception;

  /**
   * Changes the configuration of all consumer groups.
   *
   * @param groupConfigs information for all consumer groups
   * @throws Exception if failed to change consumer group configuration
   */
  void configureGroups(Iterable<? extends ConsumerGroupConfig> groupConfigs) throws Exception;
}
