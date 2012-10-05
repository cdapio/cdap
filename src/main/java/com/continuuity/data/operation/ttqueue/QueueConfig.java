package com.continuuity.data.operation.ttqueue;

import com.continuuity.data.operation.ttqueue.QueuePartitioner.PartitionerType;
import com.google.common.base.Objects;

/**
 * Configuration of a queue that is fixed for lifetime of a consumer/flowlet.
 *
 * This object is immutable and can be reused across queue operations.
 */
public class QueueConfig {

  private final PartitionerType partitionerType;
  private final boolean singleEntry;

  public QueueConfig(PartitionerType partitionerType, boolean singleEntry) {
    this.partitionerType = partitionerType;
    this.singleEntry = singleEntry;
  }

  public PartitionerType getPartitionerType() {
    return this.partitionerType;
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
        .add("partitionerType", this.partitionerType)
        .toString();
  }
}