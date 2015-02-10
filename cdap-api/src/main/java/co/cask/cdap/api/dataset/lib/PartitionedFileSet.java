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
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.dataset.Dataset;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Represents a dataset that is split into partitions that can be uniquely addressed
 * by partitioning keys along multiple dimensions. Each partition is a path in a file set,
 * the partitioning keys attached as meta data.
 *
 * This dataset can be made available for querying with SQL (explore). This is enabled through dataset
 * properties when the dataset is created. See {@link FileSetProperties}
 * for details. If it is enabled for explore, a Hive external table will be created when the dataset is
 * created. The Hive table is partitioned by the same keys as this dataset.
 */
@Beta
public interface PartitionedFileSet extends Dataset, InputFormatProvider, OutputFormatProvider {

  /**
   * Get the partitioning declared for the file set.
   */
  public Partitioning getPartitioning();

  /**
   * Add a partition for a given partition key, stored at a given path (relative to the file set's base path).
   */
  public void addPartition(PartitionKey key, String path);

  /**
   * Remove a partition for a given partition key.
   */
  public void dropPartition(PartitionKey key);

  /**
   * @return the relative path of the partition for a specific partition key.
   */
  @Nullable
  public String getPartition(PartitionKey key);

  /**
   * @return the relative paths of all partitions matching a filter.
   *
   * @param filter If non null, only partitions that match this filter are returned. If null,
   *               all partitions are returned.
   */
  public Set<String> getPartitionPaths(@Nullable PartitionFilter filter);

  /**
   * @return a mapping from the partition key to the relative path, of all partitions whose
   *  partitions keys match the filter.
   *
   * @param filter If non null, only partitions that match this filter are returned. If null,
   *               all partitions are returned.
   */
  public Map<PartitionKey, String> getPartitions(@Nullable PartitionFilter filter);

  /**
   * @return the underlying (embedded) file set.
   */
  public FileSet getEmbeddedFileSet();

  /**
   * Allow direct access to the runtime arguments of this partitioned file set.
   *
   * @return the runtime arguments specified for this dataset.
   */
  Map<String, String> getRuntimeArguments();
}
