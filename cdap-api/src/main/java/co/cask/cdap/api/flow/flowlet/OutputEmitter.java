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

package co.cask.cdap.api.flow.flowlet;

import co.cask.cdap.api.annotation.Beta;

import java.util.Map;

/**
 * This interface defines an emitter used for emitting events from
 * within a flowlet.
 *
 * @param <T> Type of data that could be emitted by this OutputEmitter.
 */
public interface OutputEmitter<T> {
  /**
   * Emits an event of type T.
   * @param data Data to be emitted by the emitter which is of type T
   */
  void emit(T data);

  /**
   * Emits an event of type T, associated with set of partitions hashes for selecting
   * downstream partitioned consumers ({@link Flowlet}).
   * @param data Data to be emitted by the emitter which is of type T
   * @param partitions Mapping from partition key to object, which the {@link Object#hashCode()}
   *                   of the object value would be triggered to compute the actual partition value.
   */
  @Beta
  void emit(T data, Map<String, Object> partitions);

  /**
   * Emits an event of type T, associated with a partition hash for selecting
   * downstream partitioned consumers ({@link Flowlet}).
   * @param data Data to be emitted by the emitter which is of type T
   * @param partitionKey Name of partition key
   * @param partitionValue The object whose {@link Object#hashCode()}
   *                       would be triggered to compute the actual partition value.
   */
  @Beta
  void emit(T data, String partitionKey, Object partitionValue);
}
