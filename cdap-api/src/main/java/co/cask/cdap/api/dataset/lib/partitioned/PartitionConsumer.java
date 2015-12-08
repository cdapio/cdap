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

package co.cask.cdap.api.dataset.lib.partitioned;

import co.cask.cdap.api.dataset.lib.Partition;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;

import java.util.List;

/**
 * Incrementally consumes new/unprocessed {@link Partition}s of a {@link PartitionedFileSet}.
 * In order to support multiple partition consumers consuming different partitions from the same PartitionedFileSet,
 * the consumePartitions method must be called in its own, short transaction before the processing of the partitions.
 * This is so that other concurrent consumers can see that the partitions have been marked as IN_PROGRESS.
 */
public interface PartitionConsumer {

  /**
   * @return a {@link PartitionConsumerResult}s containing partitions that have not yet processed.
   */
  PartitionConsumerResult consumePartitions();

  /**
   * @param limit upper limit on number of partitions to consume
   * @return a {@link PartitionConsumerResult}s containing partitions that have not yet processed.
   */
  PartitionConsumerResult consumePartitions(int limit);

  /**
   * @param acceptor defines which and how many partitions to consume
   * @return a {@link PartitionConsumerResult}s containing partitions that have not yet processed.
   */
  PartitionConsumerResult consumePartitions(PartitionAcceptor acceptor);

  /**
   * This method must be called on any partitions returned by the {@code #consumePartitions} method.
   * If a program fails to call this method for any partitions, those partitions will be 'expired' after a timeout
   * defined on the configured {@link ConsumerConfiguration}.
   *
   * @param partitions list of partitions to mark as either succeeded or failed processing
   * @param succeeded whether or not processing of the specified partitions was successful
   */
  void onFinish(List<? extends Partition> partitions, boolean succeeded);
}
