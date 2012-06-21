package com.continuuity.data.operation.ttqueue;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Objects;

/**
 * Interface used to determine whether a queue entry should be returned to a
 * given consumer.
 */
public interface QueuePartitioner {

  /**
   * Returns true if the specified entry should be emitted to the specified
   * consumer.
   * @param consumer
   * @param entryId
   * @param value
   * @return true if entry should be emitted to consumer, false if not
   */
  public boolean shouldEmit(QueueConsumer consumer, long entryId,
      byte [] value);

  public static class RandomPartitioner implements QueuePartitioner {
    @Override
    public boolean shouldEmit(QueueConsumer consumer, long entryId,
        byte [] value) {
      return true;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).toString();
    }
  }

  public static class HashPartitioner implements QueuePartitioner {
    @Override
    public boolean shouldEmit(QueueConsumer consumer, long entryId,
        byte [] value) {
      int hash = Bytes.hashCode(value);
      return (hash % consumer.getGroupSize() == consumer.getInstanceId());
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).toString();
    }
  }
}
