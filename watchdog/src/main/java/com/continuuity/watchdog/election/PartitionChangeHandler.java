package com.continuuity.watchdog.election;

import java.util.Set;

/**
 * Encapsulates logic to handle leader partition changes.
 */
public interface PartitionChangeHandler {
  /**
   * Called when the leader partitions change.
   * @param partitions new leader partitions.
   */
  void partitionsChanged(Set<Integer> partitions);
}
