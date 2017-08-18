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

package co.cask.cdap.api.schedule;

/**
 * The dataset partition trigger information to be passed to the triggered program.
 */
public class PartitionTriggerInfo extends TriggerInfo {
  private final String datasetNamespace;
  private final String datasetName;
  private final int expectedNumPartitions;
  private final int actualNumPartitions;

  public PartitionTriggerInfo(String datasetNamespace, String datasetName,
                              int expectedNumPartitions, int actualNumPartitions) {
    super(Trigger.Type.PARTITION);
    this.datasetNamespace = datasetNamespace;
    this.datasetName = datasetName;
    this.expectedNumPartitions = expectedNumPartitions;
    this.actualNumPartitions = actualNumPartitions;
  }

  /**
   * @return The namespace of the dataset specified in the trigger.
   */
  public String getDatasetNamespace() {
    return datasetNamespace;
  }

  /**
   * @return The name of the dataset specified in the trigger.
   */
  public String getDatasetName() {
    return datasetName;
  }

  /**
   * @return The least number of new dataset partitions that can satisfy the trigger.
   */
  public int getExpectedNumPartitions() {
    return expectedNumPartitions;
  }

  /**
   * @return The actual number of new dataset partitions that satisfies the trigger.
   */
  public int getActualNumPartitions() {
    return actualNumPartitions;
  }
}
