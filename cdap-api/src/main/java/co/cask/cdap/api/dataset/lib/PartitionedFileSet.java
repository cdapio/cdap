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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.PartitionNotFoundException;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Represents a dataset that is split into partitions that can be uniquely addressed
 * by partition keys along multiple dimensions. Each partition is a path in a file set,
 * the partition key attached as meta data. Note that the partitioning of the dataset
 * is fixed, that is, all operations that accept a partition key as a parameter require
 * that that key has exactly the same schema as the partitioning.
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
  Partitioning getPartitioning();

  /**
   * Add a partition for a given partition key, stored at a given path (relative to the file set's base path).
   *
   * @throws DataSetException if a partition for the same key already exists
   * @throws IllegalArgumentException if the partition key does not match the partitioning of the dataset
   */
  void addPartition(PartitionKey key, String path);

  /**
   * Add a partition for a given partition key, stored at a given path (relative to the file set's base path),
   * with the given metadata.
   *
   * @throws DataSetException if a partition for the same key already exists
   * @throws IllegalArgumentException if the partition key does not match the partitioning of the dataset
   */
  void addPartition(PartitionKey key, String path, Map<String, String> metadata);

  /**
   * Adds a new metadata entry for a particular partition.
   * Note that existing entries cannot be updated.
   * 
   * @throws DataSetException when an attempt is made to update an existing entry
   * @throws PartitionNotFoundException when a partition for the given key is not found
   * @throws IllegalArgumentException if the partition key does not match the partitioning of the dataset
   */
  void addMetadata(PartitionKey key, String metadataKey, String metadataValue);

  /**
   * Adds a set of new metadata entries for a particular partition.
   * Note that existing entries cannot be updated.
   *
   * @throws DataSetException when an attempt is made to update existing entries
   * @throws PartitionNotFoundException when a partition for the given key is not found
   * @throws IllegalArgumentException if the partition key does not match the partitioning of the dataset
   */
  void addMetadata(PartitionKey key, Map<String, String> metadata);

  /**
   * Remove a partition for a given partition key, silently ignoring if the key is not found.
   *
   * @throws IllegalArgumentException if the partition key does not match the partitioning of the dataset
   */
  void dropPartition(PartitionKey key);

  /**
   * Return the partition for a specific partition key, or null if key is not found.
   *
   * @throws IllegalArgumentException if the partition key does not match the partitioning of the dataset
   */
  @Nullable
  PartitionDetail getPartition(PartitionKey key);

  /**
   * Return all partitions matching the partition filter.
   * @param filter If non null, only partitions that match this filter are returned. If null,
   *               all partitions are returned.
   */
  Set<PartitionDetail> getPartitions(@Nullable PartitionFilter filter);

  /**
   * Incrementally consumes partitions. This method can be used to retrieve partitions that have been created since the
   * last call to this method. Note that it is the client's responsibility to maintain state of the partitions processed
   * in the iterator returned in the PartitionConsumerResult.
   *
   * @param partitionConsumerState the state from which to start consuming from
   * @return {@link PartitionConsumerResult} which holds the state of consumption as well as an iterator to the consumed
   *         {@link Partition}s
   */
  PartitionConsumerResult consumePartitions(PartitionConsumerState partitionConsumerState);

  /**
   * Incrementally consumes partitions. This method can be used to retrieve partitions that have been created since the
   * last call to this method. Note that it is the client's responsibility to maintain state of the partitions processed
   * in the iterator returned in the PartitionConsumerResult.
   *
   * @param partitionConsumerState the state from which to start consuming from
   * @param limit number of partitions, which once reached, will not add add more partitions committed by other
   *              transactions; the limit is checked after adding consuming all partitions of a transaction, so
   *              the total number of consumed partitions may be greater than this limit
   * @param predicate a predicate which determines the partitions to be consumed
   * @return {@link PartitionConsumerResult} which holds the state of consumption as well as an iterator to the consumed
   *         {@link Partition}s
   */
  PartitionConsumerResult consumePartitions(PartitionConsumerState partitionConsumerState,
                                            int limit, Predicate<PartitionDetail> predicate);

  /**
   * Return a partition output for a specific partition key, in preparation for creating a new partition.
   * Obtain the location to write from the PartitionOutput, then call the {@link PartitionOutput#addPartition}
   * to add the partition to this dataset.
   *
   * @throws IllegalArgumentException if the partition key does not match the partitioning of the dataset
   */
  PartitionOutput getPartitionOutput(PartitionKey key);

  /**
   * @return the underlying (embedded) file set.
   */
  FileSet getEmbeddedFileSet();

  /**
   * Allow direct access to the runtime arguments of this partitioned file set.
   *
   * @return the runtime arguments specified for this dataset.
   */
  Map<String, String> getRuntimeArguments();

}
