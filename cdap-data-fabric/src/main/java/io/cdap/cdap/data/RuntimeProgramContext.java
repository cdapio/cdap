/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.data;

import co.cask.cdap.api.dataset.lib.PartitionKey;

import java.io.IOException;
import java.util.Collection;

/**
 * The context that provides runtime services and information about the running program.
 */
public interface RuntimeProgramContext extends ProgramContext {

  /**
   * Notifies that a new set of partitions has been added.
   *
   * @param partitionKeys the set of partition keys being added
   * @throws IOException if failed to publish the notification
   */
  void notifyNewPartitions(Collection<? extends PartitionKey> partitionKeys) throws IOException;
}
