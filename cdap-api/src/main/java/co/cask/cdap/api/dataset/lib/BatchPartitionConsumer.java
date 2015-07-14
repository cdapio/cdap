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

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.DatasetContext;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A utility class to assist an incremental consumer of {@link Partition}s of a {@link PartitionedFileSet}. The
 * implementing class is required to handle persistence of the consumption state via the abstract readBytes and
 * writeBytes methods.
 */
@Beta
public abstract class BatchPartitionConsumer {
  private PartitionConsumerState finalConsumerState;

  /**
   * @return the serialized bytes of the state of the partition consuming batch process; return null to indicate a fresh
   * state of consuming (defaults to starting from the beginning).
   */
  @Nullable
  protected abstract byte[] readBytes(DatasetContext datasetContext);

  /**
   * Writes the serialized bytes of the state of the partition consuming batch process. The bytes written in this method
   * should be available in the following call to readBytes(DatasetContext).
   */
  protected abstract void writeBytes(DatasetContext datasetContext, byte[] stateBytes);

  /**
   * Used from the beforeSubmit method of the implementing batch job to get a PartitionedFileSet that has specified
   * a set of {@link Partition}s of a {@link PartitionedFileSet} to be processed by the run of the batch job.
   *
   * @param datasetContext dataset context used to access the PartitionedFileSet.
   * @param partitionedFileSetName the name of the PartitionedFileSet of which partitions will be processed.
   * @return a PartitionedFileSet with arguments specified such that a call to its getInputFormatConfiguration method
   * will return the paths of the Partitions that have yet to be consumed, according to the state deserialized from
   * the implementing readBytes(DatasetContext) method.
   */
  public PartitionedFileSet getPartitionedFileSet(DatasetContext datasetContext, String partitionedFileSetName) {
    byte[] stateBytes = readBytes(datasetContext);
    PartitionConsumerState partitionConsumerState =
      stateBytes == null ? PartitionConsumerState.FROM_BEGINNING : PartitionConsumerState.fromBytes(stateBytes);

    PartitionedFileSet partitionedFileSet = datasetContext.getDataset(partitionedFileSetName);
    PartitionConsumerResult partitionConsumerResult = partitionedFileSet.consumePartitions(partitionConsumerState);
    finalConsumerState = partitionConsumerResult.getPartitionConsumerState();

    Map<String, String> arguments = new HashMap<>();
    PartitionedFileSetArguments.addInputPartitions(arguments, partitionConsumerResult.getPartitionIterator());

    return datasetContext.getDataset(partitionedFileSetName, arguments);
  }

  /**
   * Used to persist the state of the PartitionConsumerState. Call this method at the end of processing the partitions.
   */
  public void onFinish(DatasetContext context) {
    writeBytes(context, finalConsumerState.toBytes());
  }
}
