/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.zookeeper.coordination;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;

import java.util.Map;
import java.util.Set;

/**
 * Describes a resource requirement for {@link ResourceCoordinator}.
 */
public final class ResourceRequirement {

  /**
   * Name of the resource.
   */
  private final String name;

  /**
   * Set of partitions contained inside this resource requirement.
   */
  private final Set<Partition> partitions;

  private ResourceRequirement(String name, Set<Partition> partitions) {
    Preconditions.checkNotNull(name, "Resource name cannot be null.");
    this.name = name;
    this.partitions = partitions;
  }

  /**
   * Returns name of this resource.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns an immutable set of {@link Partition} requirement.
   */
  public Set<Partition> getPartitions() {
    return partitions;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("name", name)
      .add("partitions", partitions)
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ResourceRequirement other = (ResourceRequirement) o;
    return name.equals(other.name) && partitions.equals(other.partitions);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, partitions);
  }

  /**
   * Describes a partition inside a {@link ResourceRequirement}.
   */
  public static final class Partition implements Comparable<Partition> {

    /**
     * Name of the partition.
     */
    private final String name;

    /**
     * Number of replications for this partition.
     */
    private final int replicas;

    public Partition(String name, int replicas) {
      this.name = name;
      this.replicas = replicas;
    }

    /**
     * Returns the name of this partition.
     */
    public String getName() {
      return name;
    }

    /**
     * Returns the number of replicas for this partition.
     */
    public int getReplicas() {
      return replicas;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Partition partition = (Partition) o;

      return name.equals(partition.name) && replicas == partition.replicas;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(name, replicas);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("name", name)
        .add("replicas", replicas)
        .toString();
    }

    @Override
    public int compareTo(Partition other) {
      int cmp = name.compareTo(other.name);
      return cmp == 0 ? Ints.compare(replicas, other.replicas) : cmp;
    }
  }

  /**
   * Creates a {@link Builder} for creating {@link ResourceRequirement}.
   *
   * @param name Name of the resource.
   */
  public static Builder builder(String name) {
    return new Builder(name);
  }

  /**
   * Builder for creation of {@link ResourceRequirement}.
   */
  public static final class Builder {

    private final String name;

    // Map from partition name to {@link Partition} object.
    private final Map<String, Partition> partitions;

    public Builder(String name) {
      this.name = name;
      this.partitions = Maps.newHashMap();
    }

    /**
     * Adds N partitions with each partition named by a prefix followed by a sequence id, starting from
     * {@code 0}. Each partition would have the same number of replicas.
     *
     * @param partitionPrefix Name prefix for partition.
     * @param numberOfPartitions Number of partitions to add.
     * @param replicasPerPartitions Number of replicas in each partition.
     *
     * @return This builder.
     */
    public Builder addPartitions(String partitionPrefix, int numberOfPartitions, int replicasPerPartitions) {
      for (int i = 0; i < numberOfPartitions; i++) {
        addPartition(new Partition(partitionPrefix + i, replicasPerPartitions));
      }
      return this;
    }

    /**
     * Adds a partition.
     *
     * @param partition Partition to add.
     * @return This builder.
     */
    public Builder addPartition(Partition partition) {
      Preconditions.checkArgument(!partitions.containsKey(partition.getName()),
                                  "Partition {} already added.", partition);
      partitions.put(partition.getName(), partition);
      return this;
    }

    public ResourceRequirement build() {
      // Sort the partitions
      return new ResourceRequirement(name, ImmutableSortedSet.copyOf(partitions.values()));
    }
  }
}
