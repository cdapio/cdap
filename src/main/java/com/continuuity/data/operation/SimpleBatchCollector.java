package com.continuuity.data.operation;

import com.continuuity.api.data.BatchCollector;
import com.continuuity.api.data.WriteOperation;

import java.util.LinkedList;
import java.util.List;

/**
 * simplest possible implementation of a batch collector. Flow can implement
 * this with its output collector.
 */
public class SimpleBatchCollector implements BatchCollector {

  List<WriteOperation> ops = new LinkedList<WriteOperation>();

  @Override
  public void add(WriteOperation write) {
    this.ops.add(write);
  }

  public List<WriteOperation> getWrites() {
    return this.ops;
  }
}
