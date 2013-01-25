package com.continuuity.data.operation;

import com.continuuity.api.data.BatchCollector;
import com.continuuity.api.data.WriteOperation;

import java.util.LinkedList;
import java.util.List;

/**
 * Simplest possible implementation of a batch collector.
 */
public class SimpleBatchCollector implements BatchCollector {

  // the write operations collected so far
  List<WriteOperation> ops = new LinkedList<WriteOperation>();

  @Override
  public void add(WriteOperation write) {
    this.ops.add(write);
  }

  /**
   * Get the write operations that were collected
   * @return the collected write operations
   */
  public List<WriteOperation> getWrites() {
    return this.ops;
  }
}
