/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.zookeeper.coordination;

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Ints;

import java.util.Collection;
import java.util.Set;

/**
 * A {@link AssignmentStrategy} that tries to balance partition replica assignment with minimum movement.
 */
public class BalancedAssignmentStrategy implements AssignmentStrategy {

  @Override
  public <T> void assign(ResourceRequirement requirement, Set<T> handlers, ResourceAssigner<T> assigner) {
    MinMaxPriorityQueue<HandlerSize<T>> handlerQueue = MinMaxPriorityQueue.create();
    Multimap<T, PartitionReplica> assignments = assigner.get();

    // Compute for each handler how many partition replica is already assigned
    for (T handler : handlers) {
      handlerQueue.add(new HandlerSize<T>(handler, assignments));
    }

    // For each unassigned partition replica in the requirement, assign it to the handler
    // with smallest partition replica assigned. It's just a heuristic to make the later balance phase doing less work.
    int totalPartitionReplica = 0;

    for (ResourceRequirement.Partition partition : requirement.getPartitions()) {
      totalPartitionReplica += partition.getReplicas();

      for (int replica = 0; replica < partition.getReplicas(); replica++) {
        if (assigner.getHandler(partition.getName(), replica) == null) {
          HandlerSize<T> handlerSize = handlerQueue.removeFirst();
          assigner.set(handlerSize.getHandler(), partition.getName(), replica);

          // After assignment, the size should get updated, hence put it back to the queue for next round usage.
          handlerQueue.add(handlerSize);
        }
      }
    }

    // Balance
    if (totalPartitionReplica > handlers.size()) {
      balance(handlerQueue, assigner, 1);
    } else {
      // Evenly distribute it to the first N handlers.
      while (handlerQueue.size() > totalPartitionReplica) {
        // If number of handler is > total partition replica,
        // there must be at least 1 handler that has nothing assigned,
        handlerQueue.removeFirst();
      }
      // Balance it evenly, and there should be no differences in number of partition replica assigned to each handler.
      balance(handlerQueue, assigner, 0);
    }
  }

  /**
   * Balance the assignment by spreading it across all handlers evenly.
   *
   * @param handlerQueue The priority queue for tracking number of resources assigned to a given handler.
   * @param assigner The assigner for changing the assignment.
   * @param maxDiff The maximum differences between the handlers that has the most resources assigned vs the one with
   *                the least resources assigned.
   */
  private <T> void balance(MinMaxPriorityQueue<HandlerSize<T>> handlerQueue,
                           ResourceAssigner<T> assigner, int maxDiff) {
    HandlerSize<T> minHandler = handlerQueue.peekFirst();
    HandlerSize<T> maxHandler = handlerQueue.peekLast();

    // Move assignment from the handler that has the most assigned partition replica to the least one, until the
    // differences is within the desired range.
    Multimap<T, PartitionReplica> assignments = assigner.get();
    while (maxHandler.getSize() - minHandler.getSize() > maxDiff) {
      PartitionReplica partitionReplica = assignments.get(maxHandler.getHandler()).iterator().next();

      // Remove min and max from the queue, and perform the reassignment.
      handlerQueue.removeFirst();
      handlerQueue.removeLast();

      assigner.set(minHandler.getHandler(), partitionReplica);

      // After assignment, the corresponding size should get updated, hence put it back to the queue for next iteration.
      handlerQueue.add(minHandler);
      handlerQueue.add(maxHandler);

      minHandler = handlerQueue.peekFirst();
      maxHandler = handlerQueue.peekLast();
    }
  }


  /**
   * This class records number of partition replica assigned to a handler. It is used for priority queue for
   * fast retrieval of handler with the min/max number of resources assigned.
   *
   * @param <T> Type of resource handler.
   */
  private static final class HandlerSize<T> implements Comparable<HandlerSize<T>> {
    private final T handler;

    // This is a live view from the assignments multimap. Updates to the multimap will update this view.
    private final Collection<PartitionReplica> assigned;

    private HandlerSize(T handler, Multimap<T, PartitionReplica> assignments) {
      this.handler = handler;
      this.assigned = assignments.get(handler);
    }

    public T getHandler() {
      return handler;
    }

    public int getSize() {
      return assigned.size();
    }

    @Override
    public int compareTo(HandlerSize<T> o) {
      return Ints.compare(getSize(), o.getSize());
    }
  }
}
