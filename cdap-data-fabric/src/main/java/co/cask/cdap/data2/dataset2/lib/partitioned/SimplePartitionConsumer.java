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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.dataset.lib.Partition;
import co.cask.cdap.api.dataset.lib.PartitionConsumerResult;
import co.cask.cdap.api.dataset.lib.PartitionConsumerState;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;

import java.util.List;
import javax.annotation.Nullable;

/**
 * A simple, consumer for {@link Partition}s of a {@link PartitionedFileSet} which maintains state in memory.
 */
public class SimplePartitionConsumer {
  private final PartitionedFileSet partitionedFileSet;
  private PartitionConsumerState partitionConsumerState;

  /**
   * Creates an instance of a SimplePartitionConsumer which begins consuming from the beginning.
   *
   * @param partitionedFileSet the PartitionedFileSet to consume from
   */
  public SimplePartitionConsumer(PartitionedFileSet partitionedFileSet) {
    this.partitionedFileSet = partitionedFileSet;
    this.partitionConsumerState = PartitionConsumerState.FROM_BEGINNING;
  }

  /**
   * @return a list of {@link Partition}s of the underlying {@link PartitionedFileSet} created since the last call
   *         to this method. This excludes partitions created in in-progress transactions including the one in which the
   *         call to this method is made.
   */
  public List<PartitionDetail> consumePartitions() {
    return consumePartitions(Integer.MAX_VALUE);
  }

  /**
   * @param limit limit to be applied while consuming partitions
   * @return a list of {@link Partition}s of the underlying {@link PartitionedFileSet} created since the last call
   *         to this method. This excludes partitions created in in-progress transactions including the one in which the
   *         call to this method is made.
   */
  public List<PartitionDetail> consumePartitions(int limit) {
    return consumePartitions(limit, new Predicate<PartitionDetail>() {
      @Override
      public boolean apply(@Nullable PartitionDetail input) {
        return true;
      }
    });
  }

  /**
   * @param limit limit to be applied while consuming partitions
   * @param predicate predicate to be applied while consuming partitions
   * @return a list of {@link Partition}s of the underlying {@link PartitionedFileSet} created since the last call
   *         to this method. This excludes partitions created in in-progress transactions including the one in which the
   *         call to this method is made.
   */
  public List<PartitionDetail> consumePartitions(int limit, Predicate<PartitionDetail> predicate) {
    PartitionConsumerResult partitionConsumerResult =
      partitionedFileSet.consumePartitions(partitionConsumerState, limit, predicate);
    partitionConsumerState = partitionConsumerResult.getPartitionConsumerState();
    return partitionConsumerResult.getPartitions();
  }
}
