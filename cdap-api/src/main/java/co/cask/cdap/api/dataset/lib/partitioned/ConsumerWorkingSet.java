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

package co.cask.cdap.api.dataset.lib.partitioned;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.PartitionConsumerState;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Keeps track of a list of partitions that are either available for consuming or are currently being consumed.
 */
public class ConsumerWorkingSet {

  private static final int VERSION = 0;

  private final List<ConsumablePartition> partitions;

  private PartitionConsumerState partitionConsumerState;

  /**
   * Constructs an empty working set.
   */
  public ConsumerWorkingSet() {
    this(PartitionConsumerState.FROM_BEGINNING,
         new ArrayList<ConsumablePartition>());
  }

  /**
   * Constructs a working set using the given PartitionConsumerState and list of ConsumablePartitions.
   */
  private ConsumerWorkingSet(PartitionConsumerState partitionConsumerState,
                                    List<ConsumablePartition> partitions) {
    this.partitionConsumerState = partitionConsumerState;
    this.partitions = partitions;
  }

  /**
   * @return the list of partitions of this working set
   */
  public List<ConsumablePartition> getPartitions() {
    return partitions;
  }

  /**
   * Adds a new partition to the working set.
   */
  public void addPartition(PartitionKey partitionKey) {
    partitions.add(new DefaultConsumablePartition(partitionKey));
  }

  /**
   * @return the ConsumablePartition with the given PartitionKey, from the working set, after removing it from
   *         the partitions list
   */
  public ConsumablePartition remove(PartitionKey partitionKey) {
    for (int i = 0; i < partitions.size(); i++) {
      if (partitionKey.equals(partitions.get(i).getPartitionKey())) {
        return partitions.remove(i);
      }
    }
    throw new IllegalArgumentException("PartitionKey not found: " + partitionKey);
  }

  /**
   * @return the ConsumablePartition with the given PartitionKey, from the working set
   */
  public ConsumablePartition lookup(PartitionKey partitionKey) {
    for (int i = 0; i < partitions.size(); i++) {
      if (partitionKey.equals(partitions.get(i).getPartitionKey())) {
        return partitions.get(i);
      }
    }
    throw new IllegalArgumentException("PartitionKey not found: " + partitionKey);
  }

  /**
   * Populates the ConsumerWorkingSet by fetching partitions from the given PartitionedFileSet.
   *
   * @param partitionedFileSet the PartitionedFileSet to fetch partitions from
   * @param configuration the ConsumerConfiguration which defines parameters for consuming
   */
  public void populate(PartitionedFileSet partitionedFileSet, ConsumerConfiguration configuration) {
    int numToPopulate = configuration.getMaxWorkingSetSize() - partitions.size();
    Predicate<PartitionDetail> predicate = configuration.getPartitionPredicate();
    co.cask.cdap.api.dataset.lib.PartitionConsumerResult result =
      partitionedFileSet.consumePartitions(partitionConsumerState, numToPopulate, predicate);
    List<PartitionDetail> partitions = result.getPartitions();
    for (PartitionDetail partition : partitions) {
      addPartition(partition.getPartitionKey());
    }
    partitionConsumerState = result.getPartitionConsumerState();
  }

  // deserializes a ConsumerWorkingSet from a byte array
  public static ConsumerWorkingSet fromBytes(byte[] bytes) {
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    byte serializationFormatVersion = bb.get();
    if (serializationFormatVersion != VERSION) {
      throw new IllegalArgumentException("Unsupported serialization format: " + serializationFormatVersion);
    }

    int numPartitions = bb.getInt();
    List<ConsumablePartition> partitions = new ArrayList<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      int consumablePartitionBytesLength = bb.getInt();
      byte[] consumablePartitionBytes = new byte[consumablePartitionBytesLength];
      bb.get(consumablePartitionBytes, 0, consumablePartitionBytesLength);
      partitions.add(DefaultConsumablePartition.fromBytes(consumablePartitionBytes));
    }

    int sizeOfMarker = bb.getInt();
    byte[] markerBytes = new byte[sizeOfMarker];
    bb.get(markerBytes);
    return new ConsumerWorkingSet(PartitionConsumerState.fromBytes(markerBytes), partitions);
  }

  // serializes this ConsumerWorkingSet into a byte array
  public byte[] toBytes() {
    // first byte for serialization format version
    int numBytes = 1;
    numBytes += Ints.BYTES;

    List<byte[]> partitionsBytes = Lists.transform(partitions, new Function<ConsumablePartition, byte[]>() {
      @Nullable
      @Override
      public byte[] apply(ConsumablePartition input) {
        // we know that all the ConsumablePartitions in the partitions list are instances of DefaultPartitionConsumer
        // because of how we construct it
        return ((DefaultConsumablePartition) input).toBytes();
      }
    });

    for (byte[] partitionBytes : partitionsBytes) {
      numBytes += Ints.BYTES;
      numBytes += partitionBytes.length;
    }

    byte[] markerBytes = partitionConsumerState.toBytes();
    numBytes += Ints.BYTES;
    numBytes += markerBytes.length;

    ByteBuffer bb = ByteBuffer.allocate(numBytes);
    bb.put((byte) VERSION);
    bb.putInt(partitions.size());
    for (byte[] partitionBytes : partitionsBytes) {
      bb.putInt(partitionBytes.length);
      bb.put(partitionBytes);
    }

    bb.putInt(markerBytes.length);
    bb.put(markerBytes);
    return bb.array();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ConsumerWorkingSet that = (ConsumerWorkingSet) o;

    return partitions.equals(that.partitions)
      && partitionConsumerState.equals(that.partitionConsumerState);

  }

  @Override
  public int hashCode() {
    int result = partitions.hashCode();
    result = 31 * result + partitionConsumerState.hashCode();
    return result;
  }
}
