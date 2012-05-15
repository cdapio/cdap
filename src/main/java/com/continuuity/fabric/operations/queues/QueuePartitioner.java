package com.continuuity.fabric.operations.queues;

import org.apache.hadoop.hbase.util.Bytes;

public interface QueuePartitioner {

  /**
   * Returns true if the specified entry should be emitted to the specified
   * consumer.
   * @param consumer
   * @param entry
   * @return true if entry should be emitted to consumer, false if not
   */
  public boolean shouldEmit(QueueConsumer consumer, QueueEntry entry);

  public static class RandomPartitioner implements QueuePartitioner {
    @Override
    public boolean shouldEmit(QueueConsumer consumer, QueueEntry entry) {
      return true;
    }
  }

  public static class HashPartitioner implements QueuePartitioner {
    @Override
    public boolean shouldEmit(QueueConsumer consumer, QueueEntry entry) {
      int hash = Bytes.hashCode(entry.getValue());
      return (hash % consumer.getGroupSize() == consumer.getConsumerId());
    }
  }
}
