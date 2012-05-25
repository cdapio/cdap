package com.continuuity.data.operation.queue;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Objects;

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

    @Override
    public String toString() {
      return Objects.toStringHelper(this).toString();
    }
  }

  public static class HashPartitioner implements QueuePartitioner {
    @Override
    public boolean shouldEmit(QueueConsumer consumer, QueueEntry entry) {
      int hash = Bytes.hashCode(entry.getValue());
      return (hash % consumer.getGroupSize() == consumer.getConsumerId());
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).toString();
    }
  }
}
