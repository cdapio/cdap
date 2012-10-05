package com.continuuity.data.operation.ttqueue;

import com.continuuity.common.utils.Bytes;
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

  public static enum PartitionerType {
    RANDOM, HASH_ON_VALUE, MODULO_LONG_VALUE;

    private static final QueuePartitioner PARTITIONER_RANDOM =
        new RandomPartitioner();
    private static final QueuePartitioner PARTITIONER_HASH =
        new HashPartitioner();
    private static final QueuePartitioner PARTITIONER_LONG_MOD =
        new LongValueHashPartitioner();
    
    public QueuePartitioner getPartitioner() {
      switch (this) {
        case RANDOM: return PARTITIONER_RANDOM;
        case HASH_ON_VALUE: return PARTITIONER_HASH;
        case MODULO_LONG_VALUE: return PARTITIONER_LONG_MOD;
        default: return PARTITIONER_RANDOM;
      }
    }
  }
  
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

  public static class LongValueHashPartitioner implements QueuePartitioner {
    @Override
    public boolean shouldEmit(QueueConsumer consumer, long entryId,
        byte [] value) {
      long val = Bytes.toLong(value);
      return (val % consumer.getGroupSize()) == consumer.getInstanceId();
    }
  }
}
