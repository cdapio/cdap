/*
 * Copyright 2014 Cask, Inc.
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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.TreeMultimap;
import org.apache.twill.discovery.Discoverable;

/**
 * Describes the resource assignment as determined by {@link ResourceCoordinator}.
 */
public final class ResourceAssignment {

  private final String name;
  private final Multimap<Discoverable, PartitionReplica> assignments;

  /**
   * Creates without any assignment.
   *
   * @param name Name of the resource.
   */
  public ResourceAssignment(String name) {
    this(name, ImmutableMultimap.<Discoverable, PartitionReplica>of());
  }

  /**
   * Creates with the given assignment.
   *
   * @param name Name of the resource.
   * @param assignments Set of assignments carries by this object.
   */
  public ResourceAssignment(String name, Multimap<Discoverable, PartitionReplica> assignments) {
    this.name = name;

    TreeMultimap<Discoverable, PartitionReplica> multimap
      = TreeMultimap.create(DiscoverableComparator.COMPARATOR, PartitionReplica.COMPARATOR);
    multimap.putAll(assignments);
    this.assignments = Multimaps.unmodifiableSortedSetMultimap(multimap);
  }

  /**
   * Returns the name of the resource.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns an immutable {@link Multimap} that contains all assignments.
   */
  public Multimap<Discoverable, PartitionReplica> getAssignments() {
    return assignments;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ResourceAssignment other = (ResourceAssignment) o;
    return name.equals(other.name) && assignments.equals(other.assignments);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, assignments);
  }
}
