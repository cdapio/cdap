package com.continuuity.data.operation.ttqueue;

import com.continuuity.data.table.ReadPointer;
import com.continuuity.data.table.VersionedColumnarTable;
import com.continuuity.hbase.ttqueue.HBQPartitioner.HBQPartitionerType;
import com.google.common.base.Objects;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Interface used to determine whether a queue entry should be returned to a
 * given consumer.
 */
public interface NewQueuePartitioner {



  public boolean isDisjoint();
  public boolean needsData();
  /**
   * Returns true if the specified entry should be emitted to the specified
   * consumer.
   * @param consumer
   * @param entryId
   * @param value
   * @return true if entry should be emitted to consumer, false if not
   */
  public boolean shouldEmit(QueueConsumer consumer, long entryId, byte[] value);

  /**
   * Returns true if the specified entry should be emitted to the specified
   * consumer.
   * @param consumer
   * @param entryId
   * @return true if entry should be emitted to consumer, false if not
   */
  public boolean shouldEmit(QueueConsumer consumer, long entryId);

  public static enum PartitionerType {
    RANDOM, HASH_ON_VALUE, MODULO_LONG_VALUE, FIFO;

    private static final NewQueuePartitioner PARTITIONER_RANDOM =
        new RandomPartitioner();
    private static final NewQueuePartitioner PARTITIONER_HASH =
        new HashPartitioner();
    private static final NewQueuePartitioner PARTITIONER_LONG_MOD =
        new LongValueHashPartitioner();
    private static final NewQueuePartitioner PARTITIONER_FIFO =
      new FifoPartitioner();

    public NewQueuePartitioner getPartitioner() {
      switch (this) {
        case RANDOM: return PARTITIONER_RANDOM;
        case HASH_ON_VALUE: return PARTITIONER_HASH;
        case MODULO_LONG_VALUE: return PARTITIONER_LONG_MOD;
        case FIFO: return PARTITIONER_FIFO;
        default: return PARTITIONER_RANDOM;
      }
    }
    
    public HBQPartitionerType toHBQ() {
      switch (this) {
        case RANDOM: return HBQPartitionerType.RANDOM;
        case HASH_ON_VALUE: return HBQPartitionerType.HASH_ON_VALUE;
        case MODULO_LONG_VALUE: return HBQPartitionerType.MODULO_LONG_VALUE;
        default: return HBQPartitionerType.RANDOM;
      }
    }
  }
  
  public static class RandomPartitioner implements NewQueuePartitioner {
    @Override
    public boolean isDisjoint() {
      return false;
    }

    @Override
    public boolean needsData() {
      return false;
    }

    @Override
    public boolean shouldEmit(QueueConsumer consumer, long entryId,
        byte [] value) {
      return true;
    }

    @Override
    public boolean shouldEmit(QueueConsumer consumer, long entryId) {
      return true;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).toString();
    }
  }

  public static class HashPartitioner implements NewQueuePartitioner {
    @Override
    public boolean isDisjoint() {
      return true;
    }

    @Override
    public boolean needsData() {
      return true;
    }

    @Override
    public boolean shouldEmit(QueueConsumer consumer, long entryId) {
      return false;
    }

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

  public static class LongValueHashPartitioner implements NewQueuePartitioner {
    @Override
    public boolean isDisjoint() {
      return true;
    }

    @Override
    public boolean needsData() {
      return true;
    }

    @Override
    public boolean shouldEmit(QueueConsumer consumer, long entryId) {
      return false;
    }

    @Override
    public boolean shouldEmit(QueueConsumer consumer, long entryId,
        byte [] value) {
      long val = Bytes.toLong(value);
      return (val % consumer.getGroupSize()) == consumer.getInstanceId();
    }
  }

  public static class FifoPartitioner implements NewQueuePartitioner {
    @Override
    public boolean isDisjoint() {
      return false;
    }

    @Override
    public boolean needsData() {
      return false;
    }

    @Override
    public boolean shouldEmit(QueueConsumer consumer, long entryId) {
      return true;
    }

    @Override
    public boolean shouldEmit(QueueConsumer consumer, long entryId, byte [] value) {
      return false;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).toString();
    }
  }

  public static class RoundRobinPartitioner implements NewQueuePartitioner {
    @Override
    public boolean isDisjoint() {
      return true;
    }

    @Override
    public boolean needsData() {
      return false;
    }

    @Override
    public boolean shouldEmit(QueueConsumer consumer, long entryId) {
      return entryId % consumer.getGroupSize() == consumer.getInstanceId();
    }

    @Override
    public boolean shouldEmit(QueueConsumer consumer, long entryId, byte [] value) {
      return false;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).toString();
    }
  }
}
