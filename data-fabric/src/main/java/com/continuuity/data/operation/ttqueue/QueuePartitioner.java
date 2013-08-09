package com.continuuity.data.operation.ttqueue;

import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.google.common.base.Objects;

import java.io.IOException;

/**
 * Interface used to determine whether a queue entry should be returned to a
 * given consumer.
 */
public interface QueuePartitioner {

  /**
   * Returns true if the specified entry should be emitted to the specified
   * consumer.
   * @return true if entry should be emitted to consumer, false if not
   */
  public boolean shouldEmit(int groupSize, int instanceId, long entryId, Integer hash);

  /**
   * Defines queue partition type.
   */
  public static enum PartitionerType {
    HASH, FIFO, ROUND_ROBIN;

    private static final QueuePartitioner PARTITIONER_HASH =
      new HashPartitioner();
    private static final QueuePartitioner PARTITIONER_FIFO =
      new FifoPartitioner();
    private static final QueuePartitioner PARTITIONER_ROUND_ROBIN =
      new RoundRobinPartitioner();

    public QueuePartitioner getPartitioner() {
      switch (this) {
        case HASH: return PARTITIONER_HASH;
        case ROUND_ROBIN: return PARTITIONER_ROUND_ROBIN;
        case FIFO: return PARTITIONER_FIFO;
        default: return PARTITIONER_FIFO;
      }
    }

    public void encode(Encoder encoder) throws IOException {
      int value;
      if (this.equals(HASH)) {
        value = 0;
      } else if (this.equals(FIFO)) {
        value = 1;
      } else if (this.equals(ROUND_ROBIN)) {
        value = 2;
      } else {
        throw new RuntimeException("Unknown partitioner type " + this.name());
      }
      encoder.writeInt(value);
    }

    public static PartitionerType decode(Decoder decoder) throws IOException {
      int value = decoder.readInt();
      if (value == 0) {
        return HASH;
      } else if (value == 1) {
        return FIFO;
      } else if (value == 2) {
        return ROUND_ROBIN;
      } else {
        throw new RuntimeException("Unknown partitioner type " + value);
      }
    }
  }

  /**
   * Implementation of hash partion for queues.
   */
  public static class HashPartitioner implements QueuePartitioner {

    @Override
    public boolean shouldEmit(int groupSize, int instanceId, long entryId, Integer hash) {
      int hashValue = hash == null ? 0 : hash;
      return (hashValue % groupSize == instanceId);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).toString();
    }
  }

  /**
   * Implementation of Fifo partition for queues.
   */
  public static class FifoPartitioner implements QueuePartitioner {

    @Override
    public boolean shouldEmit(int groupSize, int instanceId, long entryId, Integer hash) {
      return true;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).toString();
    }
  }

  /**
   * Implementation of round robin partition for queues.
   */
  public static class RoundRobinPartitioner implements QueuePartitioner {

    @Override
    public boolean shouldEmit(int groupSize, int instanceId, long entryId, Integer hash) {
      return (entryId % groupSize == instanceId);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).toString();
    }
  }
}
