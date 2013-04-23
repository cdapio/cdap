/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow.flowlet;

import com.continuuity.api.annotation.Beta;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * This interface defines an emitter used for emitting events from
 * within a flowlet.
 */
public interface OutputEmitter<T> {
  /**
   * Emits an event of type T
   * @param data to be emitted by the emitter which is of type T
   */
  void emit(T data);

  /**
   * Emits an event of type T, associated with set of partitions hashes for selecting
   * downstream partitioned consumers ({@link Flowlet}).
   * @param data to be emitted by the emitter which is of type T
   * @param partitions mapping from partition key to object, which the {@link Object#hashCode()}
   *                   of the object value would be triggered to compute the actual partition value.
   */
  @Beta
  void emit(T data, Map<String, Object> partitions);

  /**
   * Emits an event of type T, associated with a partition hash for selecting
   * downstream partitioned consumers ({@link Flowlet}).
   * @param data to be emitted by the emitter which is of type T
   * @param partitionKey name of partition key
   * @param partitionValue object, whose {@link Object#hashCode()}
   *                       would be triggered to compute the actual partition value
   */
  @Beta
  void emit(T data, String partitionKey, Object partitionValue);

  /**
   * Emits a list of events of type T, associated with set of partitions hashes for selecting
   * downstream partitioned consumers ({@link Flowlet}).
   * @param dataObjects List of events with partition hashes
   */
  @Beta
  void emit(List<DataObject<T>> dataObjects);

  /**
   * Represents an event with partition hashes that needs to be emitted by the {@link Flowlet}
   * @param <T> type of event
   */
  class DataObject<T> {
    private final T data;
    private final Map<String, Object> partitions;

    /**
     * Simple constructor with only event and no partition hashes
     * @param data event
     */
    public DataObject(T data) {
      this.data = data;
      this.partitions = ImmutableMap.of();
    }

    /**
     * Constructor with an event and a set of partition hashes
     * @param data event
     * @param partitions mapping from partition key to object, which the {@link Object#hashCode()}
     *                   of the object value would be triggered to compute the actual partition value.
     */
    public DataObject(T data, Map<String, Object> partitions) {
      this.data = data;
      this.partitions = partitions;
    }

    /**
     * Constructor with an event and a single partition hash
     * @param data event
     * @param partitionKey partition key
     * @param partitionValue an object, whose {@link Object#hashCode()} would be triggered
     *                       to compute the actual partition value
     */
    public DataObject(T data, String partitionKey, Object partitionValue) {
      this.data = data;
      this.partitions = ImmutableMap.of(partitionKey, partitionValue);
    }

    /**
     * Returns event
     * @return event
     */
    public T getData() {
      return data;
    }

    /**
     * Returns partition map
     * @return partition map
     */
    public Map<String, Object> getPartitions() {
      return partitions;
    }
  }
}
