package com.continuuity.data.operation.ttqueue;

/**
 * Represents a queue internal state information
 */
public interface QueueState {
  /**
   * Returns an approximate size of the memory occupied by the state object in bytes
   * @return approximate size of the memory occupied by the state object in bytes
   */
  int weight();
}
