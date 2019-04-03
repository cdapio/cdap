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

package co.cask.cdap.data2.dataset2.lib.partitioned;

import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionMetadata;
import com.google.common.base.Objects;

/**
 * Implementation of Partition, with associated metadata.
 */
class BasicPartitionDetail extends BasicPartition implements PartitionDetail {
  protected final PartitionMetadata metadata;

  protected BasicPartitionDetail(PartitionedFileSetDataset partitionedFileSetDataset,
                                 String relativePath, PartitionKey key, PartitionMetadata metadata) {
    super(partitionedFileSetDataset, relativePath, key);
    this.metadata = metadata;
  }

  @Override
  public PartitionMetadata getMetadata() {
    return metadata;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BasicPartitionDetail that = (BasicPartitionDetail) o;

    return Objects.equal(this.metadata, that.metadata) &&
      Objects.equal(this.relativePath, that.relativePath) &&
      Objects.equal(this.key, that.key);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(metadata, relativePath, key);
  }
}
