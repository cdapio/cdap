/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.zookeeper.coordination;

import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Tests for different {@link AssignmentStrategy}.
 */
public class AssignmentStrategyTest {

  @Test
  public void testBalance() {
    // Create a 2x2 requirement (2 partition, 2 replicas each).
    ResourceRequirement requirement = ResourceRequirement.builder("resource")
                                                         .addPartitions("p", 2, 2)
                                                         .build();

    ResourceAssigner<String> assigner =
      DefaultResourceAssigner.create(HashMultimap.<String, PartitionReplica>create());

    AssignmentStrategy strategy = new BalancedAssignmentStrategy();
    Set<String> handlers = ImmutableSet.of("h1");

    strategy.assign(requirement, handlers, assigner);
    Multimap<String, PartitionReplica> assignments = assigner.get();

    // All resources should be assigned to the only handler.
    Assert.assertEquals(4, assignments.get("h1").size());

    // Now, reassign to more than 4 handlers, it should be assigned to four handlers evenly with "h1" should be one
    // of them.
    handlers = ImmutableSet.of("h1", "h2", "h3", "h4", "h5");
    strategy.assign(requirement, handlers, assigner);

    Map<String, Collection<PartitionReplica>> result = Maps.filterValues(
      assigner.get().asMap(), new Predicate<Collection<PartitionReplica>>() {
      @Override
      public boolean apply(Collection<PartitionReplica> input) {
        return input.size() == 1;
      }
    });

    Assert.assertEquals(4, result.size());
    Assert.assertTrue(result.containsKey("h1"));

    Set<PartitionReplica> handlerOneAssignment = ImmutableSet.copyOf(result.get("h1"));

    // Now modify the requirement to have 10x2
    requirement = ResourceRequirement.builder("resource")
      .addPartitions("p", 10, 2)
      .build();

    strategy.assign(requirement, handlers, assigner);

    // Each handler should received 4 partition replica.
    for (Map.Entry<String, Collection<PartitionReplica>> entry : assigner.get().asMap().entrySet()) {
      Assert.assertEquals(4, entry.getValue().size());
    }

    // Make sure the same partition replica get assigned to multiple handler
    Assert.assertEquals(20, ImmutableSet.copyOf(assigner.get().values()).size());

    // Make sure whatever was assigned to "h1" stays in "h1".
    Assert.assertTrue(Sets.difference(handlerOneAssignment, ImmutableSet.copyOf(assigner.get().get("h1"))).isEmpty());


    // Now reduce the requirement to 7x2
    requirement = ResourceRequirement.builder("resource")
      .addPartitions("p", 6, 2)
      .build();

    // Create a new assigner with invalid partitions removed.
    Multimap<String, PartitionReplica> oldAssignments = assigner.get();
    assignments = HashMultimap.create(oldAssignments);
    Iterator<Map.Entry<String, PartitionReplica>> iterator = assignments.entries().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, PartitionReplica> entry = iterator.next();
      if (Integer.parseInt(entry.getValue().getName().substring(1)) >= 6) {
        iterator.remove();
      }
    }

    handlers = ImmutableSet.of("h1", "h2", "h3", "h4", "h5", "h6", "h7");
    assigner = DefaultResourceAssigner.create(assignments);
    strategy.assign(requirement, handlers, assigner);

    for (Map.Entry<String, Collection<PartitionReplica>> entry : assigner.get().asMap().entrySet()) {
      // Check if all the partitions that was assigned to a handler stay there.
      Collection<PartitionReplica> oldAssignment = oldAssignments.get(entry.getKey());
      if (!oldAssignment.isEmpty()) {
        Assert.assertTrue(Sets.difference(ImmutableSet.copyOf(entry.getValue()),
                                          ImmutableSet.copyOf(oldAssignment)).isEmpty());
      }
    }
  }
}
