/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.zookeeper.coordination;

import com.google.common.base.Objects;
import com.google.common.primitives.Ints;

import java.util.Comparator;

/**
 * Represents a particular partition + replica pair.
 */
public final class PartitionReplica {

  public static final Comparator<PartitionReplica> COMPARATOR = new Comparator<PartitionReplica>() {
    @Override
    public int compare(PartitionReplica o1, PartitionReplica o2) {
      int cmp = o1.getName().compareTo(o2.getName());
      return (cmp == 0) ? Ints.compare(o1.getReplicaId(), o2.getReplicaId()) : cmp;
    }
  };

  /**
   * Name of the partition.
   */
  private final String name;

  /**
   * Id for the replica inside this partition.
   */
  private final int replicaId;

  public PartitionReplica(String name, int replicaId) {
    this.name = name;
    this.replicaId = replicaId;
  }

  /**
   * Returns name of the partition.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns id for the replica inside this partition for this assignment.
   */
  public int getReplicaId() {
    return replicaId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PartitionReplica other = (PartitionReplica) o;

    return name.equals(other.name) && replicaId == other.replicaId;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, replicaId);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("partition", name)
      .add("replica", replicaId)
      .toString();
  }
}
