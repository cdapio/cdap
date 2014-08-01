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

import java.util.Set;

/**
 * Strategy for assigning service to resource.
 */
public interface AssignmentStrategy {

  /**
   * Assigns handler to {@link PartitionReplica}.
   *
   * @param requirement The requirement on the resource.
   * @param handlers Set of available handlers
   * @param assigner Use to assign partition to handler.
   */
  <T> void assign(ResourceRequirement requirement, Set<T> handlers, ResourceAssigner<T> assigner);
}
