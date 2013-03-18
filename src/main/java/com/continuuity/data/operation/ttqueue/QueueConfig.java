package com.continuuity.data.operation.ttqueue;

import com.continuuity.data.operation.ttqueue.QueuePartitioner.PartitionerType;
import com.continuuity.hbase.ttqueue.HBQConfig;
import com.google.common.base.Objects;

/**
 * Configuration of a queue that is fixed for lifetime of a consumer/flowlet.
 *
 * This object is immutable and can be reused across queue operations.
 */
public class QueueConfig {

  private final PartitionerType partitionerType;
  private final boolean singleEntry;
  private final int batchSize;

  public QueueConfig(PartitionerType partitionerType, boolean singleEntry) {
    this.partitionerType = partitionerType;
    this.singleEntry = singleEntry;
    this.batchSize = 100;
  }

  public QueueConfig(PartitionerType partitionerType, boolean singleEntry, int batchSize) {
    this.partitionerType = partitionerType;
    this.singleEntry = singleEntry;

    if(batchSize <= 0) {
      throw new IllegalArgumentException(
        String.format("batchSize has to be greater than zero, given batchSize=%d", batchSize));
    }
    this.batchSize = batchSize;
  }

  public PartitionerType getPartitionerType() {
    return this.partitionerType;
  }

  public boolean isSingleEntry() {
    return this.singleEntry;
  }

  public int getBatchSize() {
    return this.batchSize;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("singleEntry", this.singleEntry)
        .add("partitionerType", this.partitionerType)
        .add("batchSize", this.batchSize)
        .toString();
  }

  public HBQConfig toHBQ() {
    return new HBQConfig(partitionerType.toHBQ(), singleEntry);
  }
}