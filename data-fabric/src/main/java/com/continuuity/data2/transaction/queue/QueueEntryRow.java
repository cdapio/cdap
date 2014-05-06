package com.continuuity.data2.transaction.queue;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueStrategy;
import com.continuuity.data2.queue.QueueEntry;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.queue.hbase.HBaseQueueAdmin;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.hadoop.hbase.KeyValue;

import java.io.IOException;
import java.util.Arrays;
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
    if (queueName.isStream()) {
      // NOTE: stream is uniquely identified by table name
      return Bytes.EMPTY_BYTE_ARRAY;
    }
    String flowlet = queueName.getThirdComponent();
    String output = queueName.getSimpleName();
    byte[] idWithinFlow = (flowlet + "/" + output).getBytes(Charsets.US_ASCII);

    return getQueueRowPrefix(idWithinFlow);
  }

  /**
   * Returns a byte array representing prefix of a queue. The prefix is formed by first byte of
   * MD5 of the queue name followed by the queue name.
   */
  private static byte[] getQueueRowPrefix(byte[] queueIdWithinFlow) {
    byte[] bytes = new byte[queueIdWithinFlow.length + 1];
    Hashing.md5().hashBytes(queueIdWithinFlow).writeBytesTo(bytes, 0, 1);
    System.arraycopy(queueIdWithinFlow, 0, bytes, 1, queueIdWithinFlow.length);

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

  /**
   * Extracts the queue name from the KeyValue row, which the row must be a queue entry.
   */
  public static QueueName getQueueName(String appName, String flowName, KeyValue keyValue) {
    return getQueueName(appName, flowName, keyValue.getBuffer(), keyValue.getRowOffset(), keyValue.getRowLength());
  }

  /**
   * Extracts the queue name from the KeyValue row, which the row must be a queue entry.
   */
  public static QueueName getQueueName(String appName, String flowName,
                                       byte[] rowBuffer, int rowOffset, int rowLength) {
    // Entry key is always (salt bytes + 1 MD5 byte + queueName + longWritePointer + intCounter)
    int queueNameEnd = rowOffset + rowLength - Bytes.SIZEOF_LONG - Bytes.SIZEOF_INT;

    // <flowlet><source>
    byte[] idWithinFlow = Arrays.copyOfRange(rowBuffer,
                                             rowOffset + HBaseQueueAdmin.SALT_BYTES + 1,
                                             queueNameEnd);
    String idWithinFlowAsString = new String(idWithinFlow, Charsets.US_ASCII);
    // <flowlet><source>
    String[] parts = idWithinFlowAsString.split("/");

    return QueueName.fromFlowlet(appName, flowName, parts[0], parts[1]);
  }

  /**
   * Returns true if the given KeyValue row is a queue entry of the given queue based on queue row prefix
   */
  public static boolean isQueueEntry(byte[] queueRowPrefix, KeyValue keyValue) {
    return isQueueEntry(queueRowPrefix, keyValue.getBuffer(), keyValue.getRowOffset(), keyValue.getRowLength());
  }

  /**
   * Returns true if the given KeyValue row is a queue entry of the given queue based on queue row prefix
   */
  public static boolean isQueueEntry(byte[] queueRowPrefix, byte[] rowBuffer, int rowOffset, int rowLength) {
    return isPrefix(rowBuffer,
                    rowOffset + 1 + HBaseQueueAdmin.SALT_BYTES,
                    rowLength - 1 - HBaseQueueAdmin.SALT_BYTES,
                    queueRowPrefix);
  }

  /**
   * Returns {@code true} if the given {@link KeyValue} is a state column in queue entry row.
   */
  public static boolean isStateColumn(KeyValue keyValue) {
    return columnHasPrefix(keyValue, STATE_COLUMN_PREFIX);
  }

  /**
   * Returns {@code true} if the given {@link byte[]} is a state column qualifier in queue entry row.
   */
  public static boolean isStateColumn(byte[] qualifierBuffer, int qualifierOffset) {
    return columnHasPrefix(qualifierBuffer, qualifierOffset, STATE_COLUMN_PREFIX);
  }

  /**
   * Returns {@code true} if the given {@link KeyValue} is a meta column in queue entry row.
   */
  public static boolean isMetaColumn(KeyValue keyValue) {
    return columnHasPrefix(keyValue, META_COLUMN);
  }

  /**
   * Returns {@code true} if the given {@link byte[]} is a meta column qualifier in queue entry row.
   */
  public static boolean isMetaColumn(byte[] qualifierBuffer, int qualifierOffset) {
    return columnHasPrefix(qualifierBuffer, qualifierOffset, META_COLUMN);
  }

  /**
   * Returns {@code true} if the given {@link KeyValue} is a data column in queue entry row.
   */
  public static boolean isDataColumn(KeyValue keyValue) {
    return columnHasPrefix(keyValue, DATA_COLUMN);
  }

  /**
   * Returns {@code true} if the given {@link byte[]} is a data column qualifier in queue entry row.
   */
  public static boolean isDataColumn(byte[] qualifierBuffer, int qualifierOffset) {
    return columnHasPrefix(qualifierBuffer, qualifierOffset, DATA_COLUMN);
  }

  private static boolean columnHasPrefix(KeyValue keyValue, byte[] prefix) {
    return columnHasPrefix(keyValue.getBuffer(), keyValue.getQualifierOffset(), prefix);
  }

  private static boolean columnHasPrefix(byte[] qualifierBuffer, int qualifierOffset, byte[] prefix) {
    // only comparing prefix bytes so we use the prefix length for both cases
    return Bytes.equals(prefix, 0, prefix.length, qualifierBuffer, qualifierOffset, prefix.length);
  }

  private static boolean isPrefix(byte[] bytes, int off, int len, byte[] prefix) {
    int prefixLen = prefix.length;
    if (len < prefixLen) {
      return false;
    }

    int i = 0;
    while (i < prefixLen) {
      if (bytes[off++] != prefix[i++]) {
        return false;
      }
    }
    return true;
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
   * "yes" ({@link com.continuuity.data2.transaction.queue.QueueEntryRow.CanConsume#YES},
   * "no"  ({@link com.continuuity.data2.transaction.queue.QueueEntryRow.CanConsume#NO},
   * "no" with a hint that given consumer cannot consume any of the entries prior to this one
   *       ({@link com.continuuity.data2.transaction.queue.QueueEntryRow.CanConsume#NO_INCLUDING_ALL_OLDER}.
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
        return consumerConfig.getInstanceId() == Math.abs(hashValue % consumerConfig.getGroupSize()) ?
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
        // Assign to instance based on modulus on the abs(hashValue).  abs used since the hash value can be negative.
        return consumerConfig.getInstanceId() ==
          (Math.abs(hashValue) % consumerConfig.getGroupSize()) ? CanConsume.YES : CanConsume.NO;
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
