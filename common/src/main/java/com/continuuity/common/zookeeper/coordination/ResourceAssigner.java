/*
 * Copyright 2012-2014 Continuuity, Inc.
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
package com.continuuity.common.zookeeper.coordination;

import com.google.common.collect.Multimap;

/**
 * This interface is for retrieving existing assignment as well as doing new assignment. Assignment is done in a way
 * that no two handler would be handling the same {@link PartitionReplica}.
 *
 * @param <T> Type of the resource handler.
 */
public interface ResourceAssigner<T> {

  /**
   * Returns an immutable {@link Multimap} that is a view of map from handler to partition replica that is
   * currently assigned to the handler. Changes in this assigner would be reflected in the returning map.
   */
  Multimap<T, PartitionReplica> get();

  /**
   * Returns the handler that is currently assigned to handle the given partition and replica.
   *
   * @return the handler name or {@code null} if no handler is assigned.
   */
  T getHandler(String partition, int replica);

  /**
   * Assigns a particular partition replica pair to a given handler. Same as calling
   * {@link #set(Object, PartitionReplica) set(handler, new PartitionReplica(partition, replica))}.
   */
  void set(T handler, String partition, int replica);

  /**
   * Assigns a particular partition replica pair to a given handler. If there is any handler currently assigned
   * to the given {@link PartitionReplica}, it will get replaced by the new one.
   *
   * @param handler The handler to assign to.
   * @param partitionReplica The partition replica pair to assign to the given handler.
   */
  void set(T handler, PartitionReplica partitionReplica);
}
