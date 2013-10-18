package com.continuuity.data2.transaction.queue;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueStrategy;
import com.continuuity.data2.queue.QueueEntry;
import com.continuuity.data2.transaction.Transaction;
import com.google.common.base.Objects;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.io.IOException;
import java.util.Map;

/**
 * Holds logic of how queue entry row is consructed
 */
public class QueueEntryRow {
  public static final byte[] COLUMN_FAMILY = new byte[] {'q'};
  public static final byte[] DATA_COLUMN = new byte[] {'d'};
  public static final byte[] META_COLUMN = new byte[] {'m'};
  public static final byte[] STATE_COLUMN_PREFIX = new byte[] {'s'};

  /**
   * Returns a byte array representing prefix of a queue. The prefix is formed by first two bytes of
   * MD5 of the queue name followed by the queue name.
   */
  public static byte[] getQueueRowPrefix(QueueName queueName) {
    byte[] queueBytes = queueName.toBytes();
    return getQueueRowPrefix(queueBytes);
  }

  /**
   * Returns a byte array representing prefix of a queue. The prefix is formed by first two bytes of
   * MD5 of the queue name followed by the queue name.
   */
  public static byte[] getQueueRowPrefix(byte[] queueName) {
    byte[] bytes = new byte[queueName.length + 2];
    Hashing.md5().hashBytes(queueName).writeBytesTo(bytes, 0, 2);
    System.arraycopy(queueName, 0, bytes, 2, queueName.length);

    return bytes;
  }

  /**
   * Determine whether a column represent the state of a consumer.
   */
  public static boolean isStateColumn(byte[] columnName) {
    return Bytes.startsWith(columnName, QueueEntryRow.STATE_COLUMN_PREFIX);
  }

  /**
   * @param stateValue value of the state column
   * @return write pointer of the latest change of the state value
   */
  public static long getStateWritePointer(byte[] stateValue) {
    return Bytes.toLong(stateValue, 0, Longs.BYTES);
  }

  /**
   * @param stateValue value of the state column
   * @return state instance id
   */
  public static int getStateInstanceId(byte[] stateValue) {
    return Bytes.toInt(stateValue, Longs.BYTES, Ints.BYTES);
  }

  /**
   * @param stateValue value of the state column
   * @return consumer entry state
   */
  public static ConsumerEntryState getState(byte[] stateValue) {
    return ConsumerEntryState.fromState(stateValue[Longs.BYTES + Ints.BYTES]);
  }

  // Consuming logic

  /**
   * Defines if queue entry can be consumed
   */
  public static enum CanConsume {
    YES,
    NO,
    NO_INCLUDING_ALL_OLDER
  }

  /**
   * Looks at specific queue entry and determines if consumer with given consumer config and current transaction
   * can consume this entry. The answer can be
   * "yes" ({@link com.continuuity.data2.transaction.queue.QueueEntryRow.CanConsume.YES},
   * "no"  ({@link com.continuuity.data2.transaction.queue.QueueEntryRow.CanConsume.NO},
   * "no" with a hint that given consumer cannot consume any of the entries prior to this one
   *       ({@link com.continuuity.data2.transaction.queue.QueueEntryRow.CanConsume.NO_INCLUDING_ALL_OLDER}.
   * The latter one allows for some optimizations when doing scans of entries to be
   * consumed.
   *
   * @param consumerConfig config of the consumer
   * @param transaction current tx
   * @param enqueueWritePointer write pointer used by enqueue of this entry
   * @param counter counter of this entry
   * @param metaValue value of meta column of this entry
   * @param stateValue value of state column of this entry
   * @return one {@link com.continuuity.data2.transaction.queue.QueueEntryRow.CanConsume} as per description above.
   */
  public static CanConsume canConsume(ConsumerConfig consumerConfig, Transaction transaction,
                                      long enqueueWritePointer, int counter,
                                      byte[] metaValue, byte[] stateValue) {
    if (stateValue != null) {
      // If the state is written by the current transaction, ignore it, as it's processing
      long stateWritePointer = QueueEntryRow.getStateWritePointer(stateValue);
      if (stateWritePointer == transaction.getWritePointer()) {
        return CanConsume.NO;
      }

      // If the state was updated by a different consumer instance that is still active, ignore this entry.
      // The assumption is, the corresponding instance is either processing (claimed)
      // or going to process it (due to rollback/restart).
      // This only applies to FIFO, as for hash and rr, repartition needs to happen if group size change.
      int stateInstanceId = QueueEntryRow.getStateInstanceId(stateValue);
      if (consumerConfig.getDequeueStrategy() == DequeueStrategy.FIFO
        && stateInstanceId < consumerConfig.getGroupSize()
        && stateInstanceId != consumerConfig.getInstanceId()) {
        return CanConsume.NO;
      }

      // If state is PROCESSED and committed, ignore it:
      ConsumerEntryState state = QueueEntryRow.getState(stateValue);
      if (state == ConsumerEntryState.PROCESSED && transaction.isVisible(stateWritePointer)) {
        // If the entry's enqueue write pointer is smaller than smallest in progress tx, then everything before it
        // must be processed, too (it is not possible that an enqueue before this is still in progress). So it is
        // safe to move the start row after this entry.
        // Note: here we ignore the long-running transactions, because we know they don't interact with queues.
        if (enqueueWritePointer < transaction.getFirstShortInProgress()) {
          return CanConsume.NO_INCLUDING_ALL_OLDER;
        }
        return CanConsume.NO;
      }
    }

    switch (consumerConfig.getDequeueStrategy()) {
      case FIFO:
        // Always try to process (claim) if using FIFO. The resolution will be done by atomically setting state
        // to CLAIMED
        return CanConsume.YES;
      case ROUND_ROBIN: {
        int hashValue = Objects.hashCode(enqueueWritePointer, counter);
        return consumerConfig.getInstanceId() == (hashValue % consumerConfig.getGroupSize()) ?
                                                 CanConsume.YES : CanConsume.NO;
      }
      case HASH: {
        Map<String, Integer> hashKeys = null;
        try {
          hashKeys = QueueEntry.deserializeHashKeys(metaValue);
        } catch (IOException e) {
          // SHOULD NEVER happen
          throw new RuntimeException(e);
        }
        Integer hashValue = hashKeys.get(consumerConfig.getHashKey());
        if (hashValue == null) {
          // If no such hash key, default it to instance 0.
          return consumerConfig.getInstanceId() == 0 ? CanConsume.YES : CanConsume.NO;
        }
        // Assign to instance based on modulus on the hashValue.
        return consumerConfig.getInstanceId() ==
          (hashValue % consumerConfig.getGroupSize()) ? CanConsume.YES : CanConsume.NO;
      }
      default:
        throw new UnsupportedOperationException("Strategy " + consumerConfig.getDequeueStrategy() + " not supported.");
    }
  }

  /**
   * Gets the stop row for scan up to the read pointer of a transaction. Stop row is queueName + (readPointer + 1).
   */
  public static byte[] getStopRowForTransaction(byte[] queueRowPrefix, Transaction transaction) {
    return Bytes.add(queueRowPrefix, Bytes.toBytes(transaction.getReadPointer() + 1));
  }

  /**
   * For a queue entry consumer state, serialized to byte array, return whether it is processed and committed.
   */
  public static boolean isCommittedProcessed(byte[] stateBytes, Transaction tx) {
    long writePointer = Bytes.toLong(stateBytes, 0, Longs.BYTES);
    if (!tx.isVisible(writePointer)) {
      return false;
    }
    byte state = stateBytes[Longs.BYTES + Ints.BYTES];
    return state == ConsumerEntryState.PROCESSED.getState();
  }
}
