/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.zookeeper.coordination;

import java.util.Collection;

/**
 * Handle changes in resource assignment.
 */
public interface ResourceHandler {

  /**
   * Invoked when the assignment changed.
   *
   * @param partitionReplicas The new assignment.
   */
  void onChange(Collection<PartitionReplica> partitionReplicas);

  /**
   * Invoked when no more changes will be notified to this handler.
   *
   * @param failureCause Failure that causes notification stopped or {@code null} if the completion is not caused by
   *                     failure (e.g. upon request).
   */
  void finished(Throwable failureCause);
}
