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
  // if true, dequeue returns up to batchSize entries
  // if false, dequeue claims batchSize entries at once but returns only one
  private final boolean returnBatch;

  /**
   * A config without batching
   * @param partitionerType the partitioner to use
   * @param singleEntry if true, repeated dequeue returns the same element until it is ack'ed
   */
  public QueueConfig(PartitionerType partitionerType, boolean singleEntry) {
    this.partitionerType = partitionerType;
    this.singleEntry = singleEntry;
    this.batchSize = -1;
    this.returnBatch = false;
  }

  /**
   * A config with batch claim but single entry return
   * @param partitionerType the partitioner to use
   * @param singleEntry if true, repeated dequeue returns the same element until it is ack'ed
   * @param batchSize dequeue will prefetch this many entries, but return only one at a time
   */
  public QueueConfig(PartitionerType partitionerType, boolean singleEntry, int batchSize) {
    this(partitionerType, singleEntry, batchSize, false);
  }

  /**
   * A config with batch claim but single entry return
   * @param partitionerType the partitioner to use
   * @param singleEntry if true, repeated dequeue returns the same element until it is ack'ed
   * @param batchSize dequeue will (pre)fetch this many entries
   * @param returnBatch if true, dequeue will return all entries it fetches, otherwise it will return one at a time
   */
  public QueueConfig(PartitionerType partitionerType, boolean singleEntry, int batchSize, boolean returnBatch) {
    this.partitionerType = partitionerType;
    this.singleEntry = singleEntry;

    if(batchSize <= 0) {
      throw new IllegalArgumentException(
        String.format("batchSize has to be greater than zero, given batchSize=%d", batchSize));
    }
    this.batchSize = batchSize;
    this.returnBatch = returnBatch;
  }

  public PartitionerType getPartitionerType() {
    return this.partitionerType;
  }

  public boolean isSingleEntry() {
    return this.singleEntry;
  }

  public boolean returnsBatch() {
    return this.returnBatch;
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
      .add("returnBatch", this.returnBatch)
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    QueueConfig that = (QueueConfig) o;

    return batchSize == that.batchSize && returnBatch == that.returnBatch && singleEntry == that.singleEntry
      && partitionerType == that.partitionerType;

  }

  @Override
  public int hashCode() {
    int result = partitionerType != null ? partitionerType.hashCode() : 0;
    result = 31 * result + (singleEntry ? 1 : 0);
    result = 31 * result + batchSize;
    result = 31 * result + (returnBatch ? 1 : 0);
    return result;
  }

  public HBQConfig toHBQ() {
    // native HBase queues ignore batching
    return new HBQConfig(partitionerType.toHBQ(), singleEntry);
  }
}