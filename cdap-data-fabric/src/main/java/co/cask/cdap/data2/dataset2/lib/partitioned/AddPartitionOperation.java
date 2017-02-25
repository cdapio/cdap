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

package co.cask.cdap.data2.dataset2.lib.partitioned;

import co.cask.cdap.api.dataset.lib.PartitionKey;

/**
 * Represents the creation of a partition.
 */
class AddPartitionOperation extends PartitionOperation {

  private final boolean filesCreated;
  private boolean explorePartitionCreated = false;

  AddPartitionOperation(PartitionKey partitionKey, String relativePath, boolean filesCreated) {
    super(partitionKey, relativePath);
    this.filesCreated = filesCreated;
  }

  void setExplorePartitionCreated() {
    explorePartitionCreated = true;
  }

  boolean isFilesCreated() {
    return filesCreated;
  }

  boolean isExplorePartitionCreated() {
    return explorePartitionCreated;
  }
}
