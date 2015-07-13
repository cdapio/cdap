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

package co.cask.cdap.api.mapreduce;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.dataset.lib.Partition;
import co.cask.cdap.api.dataset.lib.PartitionConsumerResult;
import co.cask.cdap.api.dataset.lib.PartitionConsumerState;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import com.google.common.collect.Maps;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * An incremental consumer of {@link Partition}s of a {@link PartitionedFileSet}. The implementing class is required to
 * handle persistence of the consumption state via the abstract readBytes and writeBytes methods. Each run of this
 * MapReduce job will process newly created partitions of a PartitionedFileSet, since its previous run.
 */
@Beta
public abstract class PartitionConsumingMapReduce extends AbstractMapReduce {
  private PartitionConsumerState finalConsumerState;

  /**
   * @return the serialized bytes of the state of the partition consuming MapReduce.
   */
  @Nullable
  protected abstract byte[] readBytes(MapReduceContext mapReduceContext);

  /**
   * Writes the serialized bytes of the state of the partition consuming MapReduce. The bytes written in this method
   * should be available in the following call to readBytes(MapReduceContext).
   */
  protected abstract void writeBytes(MapReduceContext mapReduceContext, byte[] stateBytes);

  /**
   * Used from the beforeSubmit method of the implementing MapReduce job to specify {@link Partition}s of a
   * {@link PartitionedFileSet} to be processed by the run of the MapReduce job.
   *
   * @param mapReduceContext the context of the MapReduce job.
   * @param partitionedFileSetName the name of the PartitionedFileSet of which partitions will be processed.
   */
  protected void setInputPartitions(MapReduceContext mapReduceContext, String partitionedFileSetName) {
    byte[] stateBytes = readBytes(mapReduceContext);
    PartitionConsumerState partitionConsumerState =
      stateBytes == null ? PartitionConsumerState.FROM_BEGINNING : PartitionConsumerState.fromBytes(stateBytes);

    PartitionedFileSet partitionedFileSet = mapReduceContext.getDataset(partitionedFileSetName);
    PartitionConsumerResult partitionConsumerResult = partitionedFileSet.consumePartitions(partitionConsumerState);
    finalConsumerState = partitionConsumerResult.getPartitionConsumerState();

    Map<String, String> arguments = Maps.newHashMap();
    PartitionedFileSetArguments.addInputPartitions(arguments, partitionConsumerResult.getPartitionIterator());

    mapReduceContext.setInput(partitionedFileSetName, mapReduceContext.getDataset(partitionedFileSetName, arguments));
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    if (succeeded) {
      writeBytes(context, finalConsumerState.toBytes());
    }
    super.onFinish(succeeded, context);
  }
}
