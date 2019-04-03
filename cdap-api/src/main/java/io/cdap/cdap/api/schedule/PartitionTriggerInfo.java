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
public interface PartitionTriggerInfo extends TriggerInfo {
  
  /**
   * @return The namespace of the dataset specified in the trigger.
   */
  String getDatasetNamespace();

  /**
   * @return The name of the dataset specified in the trigger.
   */
  String getDatasetName();

  /**
   * @return The least number of new dataset partitions that can satisfy the trigger.
   */
  int getExpectedNumPartitions();

  /**
   * @return The actual number of new dataset partitions that satisfies the trigger.
   */
  int getActualNumPartitions();
}
