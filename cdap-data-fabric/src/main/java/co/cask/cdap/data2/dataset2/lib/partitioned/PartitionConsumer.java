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

package co.cask.cdap.data2.dataset2.lib.partitioned;

import co.cask.cdap.api.dataset.lib.Partition;
import co.cask.cdap.api.dataset.lib.PartitionConsumerResult;
import co.cask.cdap.api.dataset.lib.PartitionConsumerState;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;

import java.util.Iterator;

/**
 * A simple consumer for {@link Partition}s of a {@link PartitionedFileSet}
 */
public class PartitionConsumer {
  private final PartitionedFileSet partitionedFileSet;
  private PartitionConsumerState partitionConsumerState;

  /**
   * Creates an instance of PartitionConsumer which consumes, starting from the given PartitionConsumerState
   *
   * @param partitionedFileSet the PartitionedFileSet to consume from
   * @param partitionConsumerState the PartitionConsumerState to begin consuming from
   */
  public PartitionConsumer(PartitionedFileSet partitionedFileSet,
                           PartitionConsumerState partitionConsumerState) {
    this.partitionedFileSet = partitionedFileSet;
    this.partitionConsumerState = partitionConsumerState;
  }

  /**
   * Creates an instance of a PartitionConsumer which begins consuming from the beginning.
   *
   * @param partitionedFileSet the PartitionedFileSet to consume from
   */
  public PartitionConsumer(PartitionedFileSet partitionedFileSet) {
    this(partitionedFileSet, PartitionConsumerState.FROM_BEGINNING);
  }

  /**
   * @return an iterator to {@link Partition}s of the underlying {@link PartitionedFileSet} created since the last
   * call to this method. This excludes partitions created in in-progress transactions including the one in which the
   * call to this method is made.
   */
  public Iterator<Partition> consumePartitions() {
    PartitionConsumerResult partitionConsumerResult = partitionedFileSet.consumePartitions(partitionConsumerState);
    partitionConsumerState = partitionConsumerResult.getPartitionConsumerState();
    return partitionConsumerResult.getPartitionIterator();
  }
}
