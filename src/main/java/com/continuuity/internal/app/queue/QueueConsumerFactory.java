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
  public static class PartitionInfo {
    private final PartitionerType partitionerType;
    private final String partitionKey;

    public PartitionInfo() {
      this(PartitionerType.FIFO, null);
    }

    public PartitionInfo(PartitionerType partitionerType) {
      this(partitionerType, null);
    }

    public PartitionInfo(PartitionerType partitionerType, String partitionKey) {
      this.partitionerType = partitionerType;
      this.partitionKey = partitionKey;
    }

    public PartitionerType getPartitionerType() {
      return partitionerType;
    }

    public String getPartitionKey() {
      return partitionKey;
    }
  }
}
