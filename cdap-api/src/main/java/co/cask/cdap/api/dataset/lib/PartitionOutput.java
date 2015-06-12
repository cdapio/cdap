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

import java.util.Map;

/**
 * Represents a partition of a partitioned file set for writing.
 */
public interface PartitionOutput extends Partition {

  /**
   * Add the partition to the partitioned file set.
   */
  void addPartition();

  /**
   * Sets the metadata of a partition, when creating a new partition.
   */
  void setMetadata(Map<String, String> metadata);
}
