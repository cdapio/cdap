/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.zookeeper.coordination;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import java.util.Map;

/**
 * Implementation of {@link ResourceAssigner} that stores assignment with multimap.
 *
 * @param <T> Type of resource handler.
 */
final class DefaultResourceAssigner<T> implements ResourceAssigner<T> {

  private final Multimap<T, PartitionReplica> assignments;
  private final Map<PartitionReplica, T> replicaToHandler;

  /**
   * Constructor that takes the giving assignments as the current set of assignments.
   *
   * @param assignments Currently set of assignments. This will get modified if any of the set methods are called.
   */
  static <T> ResourceAssigner<T> create(Multimap<T, PartitionReplica> assignments) {
    return new DefaultResourceAssigner<T>(assignments);
  }

  private DefaultResourceAssigner(Multimap<T, PartitionReplica> assignments) {
    this.assignments = assignments;
    this.replicaToHandler = Maps.newHashMap();
    for (Map.Entry<T, PartitionReplica> entry : assignments.entries()) {
      replicaToHandler.put(entry.getValue(), entry.getKey());
    }
  }

  @Override
  public Multimap<T, PartitionReplica> get() {
    return Multimaps.unmodifiableMultimap(assignments);
  }

  @Override
  public T getHandler(String partition, int replica) {
    return replicaToHandler.get(new PartitionReplica(partition, replica));
  }

  @Override
  public void set(T handler, String partition, int replica) {
    // Remove existing assignment for the given partitionReplica
    set(handler, new PartitionReplica(partition, replica));
  }

  @Override
  public void set(T handler, PartitionReplica partitionReplica) {
    T oldHandler = replicaToHandler.remove(partitionReplica);
    if (oldHandler != null) {
      assignments.remove(oldHandler, partitionReplica);
    }

    assignments.put(handler, partitionReplica);
    replicaToHandler.put(partitionReplica, handler);
  }
}
