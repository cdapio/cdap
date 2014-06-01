/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.zookeeper.coordination;

/**
 * Listener to watch for changes in assignments.
 */
public interface AssignmentChangeListener {

  /**
   * Invoked when there is change in assignment.
   *
   * @param assignment The updated assignment.
   */
  void onChange(ResourceAssignment assignment);

  /**
   * Invoked when no more changes will be notified to this listener.
   *
   * @param failureCause Failure that causes notification stopped or {@code null} if the completion is not caused by
   *                     failure (e.g. upon request).
   */
  void finished(Throwable failureCause);
}
