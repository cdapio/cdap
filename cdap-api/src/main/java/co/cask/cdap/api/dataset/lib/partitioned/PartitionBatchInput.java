/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.dataset.lib.DatasetStatePersistor;
import co.cask.cdap.api.dataset.lib.Partition;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.mapreduce.MapReduceContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A utility class for batch processing (i.e. MapReduce). It works by exposing functionality to configure a
 * {@link PartitionedFileSet} as input to a MapReduceContext with runtime arguments to appropriately process partitions.
 */
@Beta
public class PartitionBatchInput {

  /**
   * See {@link #setInput(MapReduceContext, String, DatasetStatePersistor, ConsumerConfiguration)},
   * but using {@link ConsumerConfiguration#DEFAULT}.
   */
  public static BatchPartitionCommitter setInput(MapReduceContext mapreduceContext,
                                                 String partitionedFileSetName,
                                                 DatasetStatePersistor statePersistor) {
    return setInput(mapreduceContext, partitionedFileSetName, statePersistor, ConsumerConfiguration.DEFAULT);
  }

  /**
   * Used from the beforeSubmit method of the implementing batch job to configure as input a PartitionedFileSet that has
   * specified a set of {@link Partition}s of a {@link PartitionedFileSet} to be processed by the run of the batch job.
   * It does this by reading back the previous state, determining the new partitions to read, computing the new
   * state, and persisting this new state. It then configures this dataset as input to the mapreduce context that is
   * passed in.
   *
   * @param mapreduceContext MapReduce context used to access the PartitionedFileSet, and on which the input is
   *                         configured
   * @param partitionedFileSetName the name of the {@link PartitionedFileSet} to consume partitions from
   * @param statePersistor a {@link DatasetStatePersistor} responsible for defining how the partition consumer state is
   *                       managed
   * @param consumerConfiguration defines parameters for the partition consumption
   * @return a BatchPartitionCommitter used to persist the state of the partition consumer
   */
  public static BatchPartitionCommitter setInput(MapReduceContext mapreduceContext,
                                                 String partitionedFileSetName,
                                                 DatasetStatePersistor statePersistor,
                                                 ConsumerConfiguration consumerConfiguration) {
    PartitionedFileSet partitionedFileSet = mapreduceContext.getDataset(partitionedFileSetName);
    final PartitionConsumer partitionConsumer =
      new ConcurrentPartitionConsumer(partitionedFileSet,
                                      new DelegatingStatePersistor(mapreduceContext, statePersistor),
                                      consumerConfiguration);

    final List<PartitionDetail> consumedPartitions = partitionConsumer.consumePartitions().getPartitions();

    Map<String, String> arguments = new HashMap<>();
    PartitionedFileSetArguments.addInputPartitions(arguments, consumedPartitions);
    mapreduceContext.addInput(Input.ofDataset(partitionedFileSetName, arguments));

    return new BatchPartitionCommitter() {
      @Override
      public void onFinish(boolean succeeded) {
        partitionConsumer.onFinish(consumedPartitions, succeeded);
      }
    };
  }

  /**
   * Used to persist the state of the partition consumer. Call this method at the end of processing the partitions.
   */
  public interface BatchPartitionCommitter {
    void onFinish(boolean succeeded);
  }
}
