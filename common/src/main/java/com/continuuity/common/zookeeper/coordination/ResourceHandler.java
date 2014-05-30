/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.zookeeper.coordination;

import com.google.common.collect.ImmutableList;
import org.apache.twill.discovery.Discoverable;

import java.util.Collection;

/**
 * Handle changes in resource assignment. It takes resource assignment changes and invoke changes specific to
 * the {@link Discoverable} given in the constructor.
 */
public abstract class ResourceHandler implements AssignmentChangeListener {

  private final Discoverable discoverable;
  private Collection<PartitionReplica> oldAssignment;

  protected ResourceHandler(Discoverable discoverable) {
    this.discoverable = discoverable;
  }

  /**
   * Invoked when the assignment changed.
   *
   * @param partitionReplicas The new assignment.
   */
  protected abstract void onChange(Collection<PartitionReplica> partitionReplicas);


  @Override
  public final void onChange(ResourceAssignment assignment) {
    // If service name is different, ignore it
    if (!assignment.getName().equals(discoverable.getName())) {
      return;
    }

    // For each new assignment, see if it has been changed by comparing with the old assignment.
    Collection<PartitionReplica> newAssignment = assignment.getAssignments().get(discoverable);
    if (oldAssignment == null || !oldAssignment.equals(newAssignment)) {
      // Notify
      onChange(newAssignment);
    }

    oldAssignment = assignment.getAssignments().get(discoverable);
  }

  protected final Discoverable getDiscoverable() {
    return discoverable;
  }
}
