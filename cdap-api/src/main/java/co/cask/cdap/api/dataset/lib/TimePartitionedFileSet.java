/*
 * Copyright © 2014 Cask Data, Inc.
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

import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Represents a dataset that is split into partitions that can be uniquely addressed
 * by time. Each partition is a path in a file set, with a timestamp attached as meta data.
 *
 * This dataset can be made available for querying with SQL (explore). This is enabled through dataset
 * properties when the dataset is created. See {@link co.cask.cdap.api.dataset.lib.FileSetProperties}
 * for details. If it is enabled for explore, a Hive external table will be created when the dataset is
 * created. The Hive table is partitioned by year, month, day, hour and minute.
 */
@Beta
public interface TimePartitionedFileSet extends Dataset, InputFormatProvider, OutputFormatProvider {

  /**
   * Add a partition for a given time, stored at a given path (relative to the file set's base path).
   */
  public void addPartition(long time, String path);

  /**
   * Remove a partition for a given time.
   */
  public void removePartition(long time);

  /**
   * @return the relative path of the partition for a specific time.
   */
  @Nullable
  public String getPartition(long time);

  /**
   * @return the relative paths of all partitions with a time that is between startTime (inclusive)
   *         and endTime (exclusive).
   */
  public Collection<String> getPartitions(long startTime, long endTime);

  /**
   * @return the underlying (embedded) file set.
   */
  public FileSet getUnderlyingFileSet();

  /**
   * Allow direct access to the runtime arguments of this partitioned file set.
   *
   * @return the runtime arguments specified for this dataset.
   */
  Map<String, String> getRuntimeArguments();
}
