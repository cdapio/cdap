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

package io.cdap.cdap.data2.dataset2.lib.partitioned;

import com.google.common.base.Objects;
import io.cdap.cdap.api.dataset.lib.Partition;
import io.cdap.cdap.api.dataset.lib.PartitionKey;
import org.apache.twill.filesystem.Location;

/**
 * Simple Implementation of Partition.
 */
class BasicPartition implements Partition {

  protected final transient PartitionedFileSetDataset partitionedFileSetDataset;
  protected final String relativePath;
  protected final PartitionKey key;

  protected BasicPartition(PartitionedFileSetDataset partitionedFileSetDataset, String relativePath,
      PartitionKey key) {
    this.partitionedFileSetDataset = partitionedFileSetDataset;
    this.relativePath = relativePath;
    this.key = key;
  }

  @Override
  public Location getLocation() {
    return partitionedFileSetDataset.getEmbeddedFileSet().getLocation(relativePath);
  }

  @Override
  public String getRelativePath() {
    return relativePath;
  }

  @Override
  public PartitionKey getPartitionKey() {
    return key;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !(o instanceof BasicPartition)) {
      return false;
    }
    BasicPartition that = (BasicPartition) o;
    return key.equals(that.key) && relativePath.equals(that.relativePath);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(key, relativePath);
  }
}
