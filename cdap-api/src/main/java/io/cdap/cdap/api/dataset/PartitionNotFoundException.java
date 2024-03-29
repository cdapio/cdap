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

package io.cdap.cdap.api.dataset;

import io.cdap.cdap.api.dataset.lib.PartitionKey;

/**
 * Thrown when a {@link io.cdap.cdap.api.dataset.lib.Partition} is not found on a
 * PartitionedFileSet.
 */
public class PartitionNotFoundException extends DataSetException {

  private final PartitionKey partitionKey;
  private final String partitionedFileSetName;

  public PartitionNotFoundException(PartitionKey partitionKey, String partitionedFileSetName) {
    super(
        String.format("Dataset '%s' does not have a partition for key: %s", partitionedFileSetName,
            partitionKey));
    this.partitionKey = partitionKey;
    this.partitionedFileSetName = partitionedFileSetName;
  }

  public PartitionKey getPartitionKey() {
    return partitionKey;
  }

  public String getPartitionedFileSetName() {
    return partitionedFileSetName;
  }
}
