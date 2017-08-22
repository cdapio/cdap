/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.PartitionNotFoundException;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Represents a dataset that is split into partitions that can be uniquely addressed
 * by time. Each partition is a path in a file set, with a timestamp attached as meta data.
 * The timestamp is mapped to a partition key of a {@link co.cask.cdap.api.dataset.lib.PartitionedFileSet}
 * with five integer partitioning fields: the year, month, day, hour and minute. Partitions can
 * be retrieved using time range or using a {@link co.cask.cdap.api.dataset.lib.PartitionFilter}.
 * <p>
 * The granularity of time is in minutes, that is, any seconds or milliseconds after the
 * full minute is ignored for the partition keys. That means, there can not be be two partitions
 * in the same minute. Also, when retrieving partitions via time or time range using
 * {@link #getPartitionByTime}, {@link #getPartitionsByTime}, or when writing a partition using
 * {@link #getPartitionOutput}, the seconds and milliseconds on the
 * time or time range are ignored.
 * <p>
 * This dataset can be made available for querying with SQL (explore). This is enabled through dataset
 * properties when the dataset is created. See {@link co.cask.cdap.api.dataset.lib.FileSetProperties}
 * for details. If it is enabled for explore, a Hive external table will be created when the dataset is
 * created. The Hive table is partitioned by year, month, day, hour and minute.
 */
@Beta
public interface TimePartitionedFileSet extends PartitionedFileSet {

  /**
   * Add a partition for a given time, stored at a given path (relative to the file set's base path).
   *
   * @param time the partition time in milliseconds since the Epoch
   * @throws PartitionAlreadyExistsException if the partition for the given time already exists
   */
  void addPartition(long time, String path);

  /**
   * Add a partition for a given time, stored at a given path (relative to the file set's base path),
   * with given metadata.
   *
   * @param time the partition time in milliseconds since the Epoch
   * @throws PartitionAlreadyExistsException if the partition for the given time already exists
   */
  void addPartition(long time, String path, Map<String, String> metadata);

  /**
   * Adds a new metadata entry for a particular partition.
   * Note that existing entries can not be updated.
   *
   * @param time the partition time in milliseconds since the Epoch
   *
   * @throws DataSetException in case an attempt is made to update existing entries.
   */
  void addMetadata(long time, String metadataKey, String metadataValue);

  /**
   * Adds a set of new metadata entries for a particular partition
   * Note that existing entries can not be updated.
   *
   * @param time the partition time in milliseconds since the Epoch
   *
   * @throws DataSetException in case an attempt is made to update existing entries.
   */
  void addMetadata(long time, Map<String, String> metadata);

  /**
   * Sets metadata entries for a particular partition. If the metadata entry key does not already exist, it will be
   * created; otherwise, it will be overwritten. Other existing keys remain unchanged.
   *
   * @throws PartitionNotFoundException when a partition for the given key is not found
   * @throws IllegalArgumentException if the partition key does not match the partitioning of the dataset
   */
  void setMetadata(long time, Map<String, String> metadata);

  /**
   * Removes a metadata entry for a particular time.
   * If the metadata key does not exist, no error is thrown.
   *
   * @throws PartitionNotFoundException when a partition for the given time is not found
   */
  void removeMetadata(long time, String metadataKey);

  /**
   * Removes a set of metadata entries for a particular time.
   * If any metadata key does not exist, no error is thrown.
   *
   * @throws PartitionNotFoundException when a partition for the given time is not found
   */
  void removeMetadata(long time, Set<String> metadataKeys);

  /**
   * Remove a partition for a given time.
   *
   * @param time the partition time in milliseconds since the Epoch
   */
  void dropPartition(long time);

  /**
   * Return the partition associated with the given time, rounded to the minute;
   * or null if no such partition exists.
   *
   * @param time the partition time in milliseconds since the Epoch
   */
  @Nullable
  TimePartitionDetail getPartitionByTime(long time);

  /**
   * Return all partitions within the time range given by startTime (inclusive) and endTime (exclusive),
   * both rounded to the full minute.
   *
   * @param startTime the inclusive lower bound for the partition time in milliseconds since the Epoch
   * @param endTime the exclusive upper bound for the partition time in milliseconds since the Epoch
   */
  Set<TimePartitionDetail> getPartitionsByTime(long startTime, long endTime);

  /**
   * Return a partition output for a specific time, rounded to the minute, in preparation for creating a new partition.
   * Obtain the location to write from the PartitionOutput, then call the {@link PartitionOutput#addPartition}
   * to add the partition to this dataset.
   *
   * @param time the partition time in milliseconds since the Epoch
   * @throws PartitionAlreadyExistsException if the partition for the given time already exists
   */
  TimePartitionOutput getPartitionOutput(long time);
}
