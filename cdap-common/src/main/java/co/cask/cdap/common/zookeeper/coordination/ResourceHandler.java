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
package co.cask.cdap.common.zookeeper.coordination;

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
