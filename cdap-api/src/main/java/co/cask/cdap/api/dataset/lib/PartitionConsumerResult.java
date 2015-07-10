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

package co.cask.cdap.api.dataset.lib;

import java.util.Iterator;

/**
 * Returns access to an iterator of the requested partitions as well as a {@link PartitionConsumerState} which can be
 * used to request partitions created after the previous request of partitions.
 */
public class PartitionConsumerResult {
  private final PartitionConsumerState partitionConsumerState;
  private final Iterator<Partition> partitionIterator;

  public PartitionConsumerResult(PartitionConsumerState partitionConsumerState, Iterator<Partition> partitionIterator) {
    this.partitionConsumerState = partitionConsumerState;
    this.partitionIterator = partitionIterator;
  }

  public PartitionConsumerState getPartitionConsumerState() {
    return partitionConsumerState;
  }

  public Iterator<Partition> getPartitionIterator() {
    return partitionIterator;
  }
}
