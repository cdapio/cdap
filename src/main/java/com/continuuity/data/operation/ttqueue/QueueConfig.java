package com.continuuity.data.operation.ttqueue;

import com.google.common.base.Objects;

/**
 * Configuration of a queue that is fixed for lifetime of a consumer/flowlet.
 */
public class QueueConfig {

  private final QueuePartitioner partitioner;
  private final boolean singleEntry;

  QueueConfig(QueuePartitioner partitioner, boolean singleEntry) {
    this.partitioner = partitioner;
    this.singleEntry = singleEntry;
  }

  public QueuePartitioner getPartitioner() {
    return this.partitioner;
  }

  public boolean isSingleEntry() {
    return this.singleEntry;
  }

  public boolean isMultiEntry() {
    return !this.singleEntry;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("singleEntry", this.singleEntry)
        .add("partitioner", this.partitioner)
        .toString();
  }
}