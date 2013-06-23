package com.continuuity.internal.app.queue;

import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueuePartitioner.PartitionerType;

/**
 *  A factory interface to create QueueConsumer
 */
public interface QueueConsumerFactory {

  /**
   * Creates a QueueConsumer with the given groupSize, and runs a QueueConfigure with the new QueueConsumer.
   * @param groupSize Size of the group of which the created QueueConsumer will be part of
   * @return Created QueueConsumer
   */
  QueueConsumer create(int groupSize);

  /**
   * Represents partitioning information of a Flowlet's process method
   */
  public static class QueueInfo {
    private final PartitionerType partitionerType;
    private final String partitionKey;
    private int batchSize;
    private boolean batchMode;

    public QueueInfo() {
      this(PartitionerType.FIFO, null);
    }

    public QueueInfo(PartitionerType partitionerType) {
      this(partitionerType, null);
    }

    public QueueInfo(PartitionerType partitionerType, String partitionKey) {
      this.partitionerType = partitionerType;
      this.partitionKey = partitionKey;
      this.batchSize = -1;
      this.batchMode = false;
    }

    public void setBatchSize(int batchSize) {
      this.batchSize = batchSize;
    }

    public void setBatchMode(boolean batchMode) {
      this.batchMode = batchMode;
    }

    public PartitionerType getPartitionerType() {
      return partitionerType;
    }

    public String getPartitionKey() {
      return partitionKey;
    }

    public int getBatchSize() {
      return batchSize;
    }

    public boolean isBatchMode() {
      return batchMode;
    }
  }
}
