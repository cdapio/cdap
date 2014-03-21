/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
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
