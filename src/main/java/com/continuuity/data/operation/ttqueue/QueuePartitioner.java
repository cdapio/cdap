package com.continuuity.data.operation.ttqueue;

import com.continuuity.hbase.ttqueue.HBQPartitioner.HBQPartitionerType;
import com.google.common.base.Objects;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Interface used to determine whether a queue entry should be returned to a
 * given consumer.
 */
public interface QueuePartitioner {



  public boolean isDisjoint();
  public boolean usesHeaderData();
  /**
   * Returns true if the specified entry should be emitted to the specified
   * consumer.
   * @return true if entry should be emitted to consumer, false if not
   */
  public boolean shouldEmit(QueueConsumer consumer, long entryId, byte[] value);

  public boolean shouldEmit(QueueConsumer consumer, long entryId, int hash);

  /**
   * Returns true if the specified entry should be emitted to the specified
   * consumer.
   * @return true if entry should be emitted to consumer, false if not
   */
  public boolean shouldEmit(QueueConsumer consumer, long entryId);

  public static enum PartitionerType {
    HASH_ON_VALUE, FIFO, ROUND_ROBIN;

    private static final QueuePartitioner PARTITIONER_HASH =
      new HashPartitioner();
    private static final QueuePartitioner PARTITIONER_FIFO =
      new FifoPartitioner();
    private static final QueuePartitioner PARTITIONER_ROUND_ROBIN =
      new RoundRobinPartitioner();

    public QueuePartitioner getPartitioner() {
      switch (this) {
        case HASH_ON_VALUE: return PARTITIONER_HASH;
        case ROUND_ROBIN: return PARTITIONER_ROUND_ROBIN;
        case FIFO: return PARTITIONER_FIFO;
        default: return PARTITIONER_FIFO;
      }
    }

    public HBQPartitionerType toHBQ() {
      switch (this) {
        case FIFO: return HBQPartitionerType.RANDOM; // TODO whatever we do with HBQ, rename this too
        case HASH_ON_VALUE: return HBQPartitionerType.HASH_ON_VALUE;
        default: return HBQPartitionerType.RANDOM;
      }
    }
  }

  public static class HashPartitioner implements QueuePartitioner {
    @Override
    public boolean isDisjoint() {
      return true;
    }

    @Override
    public boolean usesHeaderData() {
      return true;
    }

    @Override
    public boolean shouldEmit(QueueConsumer consumer, long entryId) {
      return false;
    }

    @Override
    public boolean shouldEmit(QueueConsumer consumer, long entryId, byte [] value) {
      int hash = Bytes.hashCode(value);
      return (hash % consumer.getGroupSize() == consumer.getInstanceId());
    }

    @Override
    public boolean shouldEmit(QueueConsumer consumer, long entryId, int hash) {
      return (hash % consumer.getGroupSize() == consumer.getInstanceId());
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).toString();
    }
  }

  public static class FifoPartitioner implements QueuePartitioner {
    @Override
    public boolean isDisjoint() {
      return false;
    }

    @Override
    public boolean usesHeaderData() {
      return false;
    }

    @Override
    public boolean shouldEmit(QueueConsumer consumer, long entryId) {
      return true;
    }

    @Override
    public boolean shouldEmit(QueueConsumer consumer, long entryId, byte [] value) {
      return true;
    }

    @Override
    public boolean shouldEmit(QueueConsumer consumer, long entryId, int hash) {
      return true;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).toString();
    }
  }

  public static class RoundRobinPartitioner implements QueuePartitioner {
    @Override
    public boolean isDisjoint() {
      return true;
    }

    @Override
    public boolean usesHeaderData() {
      return false;
    }

    @Override
    public boolean shouldEmit(QueueConsumer consumer, long entryId) {
      return entryId % consumer.getGroupSize() == consumer.getInstanceId();
    }

    @Override
    public boolean shouldEmit(QueueConsumer consumer, long entryId, byte [] value) {
      return entryId % consumer.getGroupSize() == consumer.getInstanceId();
    }

    @Override
    public boolean shouldEmit(QueueConsumer consumer, long entryId, int hash) {
      return false;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).toString();
    }
  }
}
