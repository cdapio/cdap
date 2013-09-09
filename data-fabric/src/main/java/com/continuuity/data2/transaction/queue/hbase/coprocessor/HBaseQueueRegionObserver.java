/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.hbase.coprocessor;

import com.continuuity.data2.transaction.queue.ConsumerEntryState;
import com.continuuity.data2.transaction.queue.QueueConstants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * RegionObserver for queue table. This class should only have JSE and HBase classes dependencies only.
 * It can also has dependencies on continuuity classes provided that all the transitive dependencies stay within
 * the mentioned scope.
 *
 * This region observer does queue eviction during flush time and compact time by using queue consumer state
 * information to determine if a queue entry row can be omitted during flush/compact.
 */
public final class HBaseQueueRegionObserver extends BaseRegionObserver {

  private static final Log LOG = LogFactory.getLog(HBaseQueueRegionObserver.class);

  private static final int LONG_BYTES = Long.SIZE / Byte.SIZE;
  private static final int INT_BYTES = Integer.SIZE / Byte.SIZE;

  // Only queue is eligible for eviction, stream is not.
  private static final byte[] QUEUE_BYTES = Bytes.toBytes("queue");

  @Override
  public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> e,
                                  Store store, InternalScanner scanner) throws IOException {
    if (!e.getEnvironment().getRegion().isAvailable()) {
      return scanner;
    }

    LOG.info("preFlush, creates EvictionInternalScanner");
    return new EvictionInternalScanner(e.getEnvironment(), scanner);
  }

  @Override
  public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,
                                    Store store, InternalScanner scanner) throws IOException {
    if (!e.getEnvironment().getRegion().isAvailable()) {
      return scanner;
    }

    LOG.info("preCompact, creates EvictionInternalScanner");
    return new EvictionInternalScanner(e.getEnvironment(), scanner);
  }

  /**
   * An {@link InternalScanner} that will skip queue entries that are safe to be evicted.
   */
  private static final class EvictionInternalScanner implements InternalScanner {

    private final RegionCoprocessorEnvironment env;
    private final InternalScanner scanner;
    // This is just for object reused to reduce objects creation.
    private final ConsumerInstance consumerInstance;
    private byte[] currentQueue;
    private QueueConsumerConfig consumerConfig;
    private long rowsEvicted = 0;

    private EvictionInternalScanner(RegionCoprocessorEnvironment env, InternalScanner scanner) {
      this.env = env;
      this.scanner = scanner;
      this.consumerInstance = new ConsumerInstance(0, 0);
    }

    @Override
    public boolean next(List<KeyValue> results) throws IOException {
      return next(results, -1, null);
    }

    @Override
    public boolean next(List<KeyValue> results, String metric) throws IOException {
      return next(results, -1, metric);
    }

    @Override
    public boolean next(List<KeyValue> results, int limit) throws IOException {
      return next(results, limit, null);
    }

    @Override
    public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
      boolean hasNext = scanner.next(result, limit, metric);

      while (!result.isEmpty()) {
        // Check if it is eligible for eviction.
        KeyValue keyValue = result.get(0);

        // If current queue is unknown or the row is not a queue entry of current queue,
        // it either because it scans into next queue entry or simply current queue is not known.
        // Hence needs to find the currentQueue
        if (currentQueue == null || !isQueueEntry(currentQueue, keyValue)) {
          // If not eligible, it either because it scans into next queue entry or simply current queue is not known.
          currentQueue = null;

          // Either case, it checks if current row is a queue entry of QUEUE type (not STREAM).
          if (!isQueueEntry(keyValue)) {
            return hasNext;
          }
        }

        // This row is a queue entry. If currentQueue is null, meaning it's a new queue encountered during scan.
        if (currentQueue == null) {
          currentQueue = getQueueName(keyValue);
          consumerConfig = getConsumerConfig(currentQueue);
        }

        if (canEvict(consumerConfig, result)) {
          rowsEvicted++;
          result.clear();
          hasNext = scanner.next(result, limit, metric);
        } else {
          break;
        }
      }

      return hasNext;
    }

    @Override
    public void close() throws IOException {
      LOG.info("Rows evicted: " + rowsEvicted);
      scanner.close();
    }

    /**
     * Gets consumers configuration for the given queue.
     */
    private QueueConsumerConfig getConsumerConfig(byte[] queueName) {
      try {
        // Fetch the queue consumers information
        HTableInterface hTable = env.getTable(env.getRegion().getTableDesc().getName());
        try {
          Get get = new Get(queueName);
          get.addFamily(QueueConstants.COLUMN_FAMILY);

          Result result = hTable.get(get);
          Map<ConsumerInstance, byte[]> consumerInstances = new HashMap<ConsumerInstance, byte[]>();

          if (result == null || result.isEmpty()) {
            // If there is no consumer config, meaning no one is using the queue, numGroup == 0 will trigger eviction.
            return new QueueConsumerConfig(consumerInstances, 0);
          }

          NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(QueueConstants.COLUMN_FAMILY);
          if (familyMap == null) {
            // If there is no consumer config, meaning no one is using the queue, numGroup == 0 will trigger eviction.
            return new QueueConsumerConfig(consumerInstances, 0);
          }

          // Gather the startRow of all instances across all consumer groups.
          int numGroups = 0;
          Long groupId = null;
          for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
            long gid = Bytes.toLong(entry.getKey());
            int instanceId = Bytes.toInt(entry.getKey(), LONG_BYTES);
            consumerInstances.put(new ConsumerInstance(gid, instanceId), entry.getValue());

            // Columns are sorted by groupId, hence if it change, then numGroups would get +1
            if (groupId == null || groupId.longValue() != gid) {
              numGroups++;
              groupId = gid;
            }
          }

          return new QueueConsumerConfig(consumerInstances, numGroups);

        } finally {
          hTable.close();
        }
      } catch (IOException e) {
        // If there is exception when fetching consumers information,
        // uses empty map and -ve int as the consumer config to avoid eviction.
        LOG.error("Exception when looking up smallest row key for " + Bytes.toStringBinary(queueName), e);
        return new QueueConsumerConfig(new HashMap<ConsumerInstance, byte[]>(), -1);
      }
    }

    private boolean isPrefix(byte[] bytes, int off, int len, byte[] prefix) {
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

    /**
     * Extracts the queue name from the KeyValue row, which the row must be a queue entry.
     */
    private byte[] getQueueName(KeyValue keyValue) {
      // Entry key is always (2 MD5 bytes + queueName + longWritePointer + intCounter)
      int queueNameEnd = keyValue.getRowOffset() + keyValue.getRowLength() - LONG_BYTES - INT_BYTES;
      return Arrays.copyOfRange(keyValue.getBuffer(), keyValue.getRowOffset() + 2, queueNameEnd);
    }

    /**
     * Returns true if the given KeyValue row is a queue entry of the given queue.
     */
    private boolean isQueueEntry(byte[] queueName, KeyValue keyValue) {
      return isPrefix(keyValue.getBuffer(), keyValue.getRowOffset() + 2, keyValue.getRowLength() - 2, queueName);
    }

    /**
     * Returns true if the given KeyValue row is a queue entry, regardless what queue it is.
     */
    private boolean isQueueEntry(KeyValue keyValue) {
      // Only match the type of the queue.
      return isQueueEntry(QUEUE_BYTES, keyValue);
    }

    /**
     * Determines the given queue entry row can be evicted.
     * @param result All KeyValues of a queue entry row.
     * @return true if it can be evicted, false otherwise.
     */
    private boolean canEvict(QueueConsumerConfig consumerConfig, List<KeyValue> result) {
      // If no consumer group, this queue is dead, should be ok to evict.
      if (consumerConfig.getNumGroups() == 0) {
        return true;
      }

      // If unknown consumer config (due to error), keep the queue.
      if (consumerConfig.getNumGroups() < 0) {
        return false;
      }

      // TODO (terence): Right now we can only evict if we see all the data columns.
      // It's because it's possible that in some previous flush, only the data columns are flush,
      // then consumer writes the state columns. In the next flush, it'll only see the state columns and those
      // should not be evicted otherwise the entry might get reprocessed, depending on the consumer start row state.
      // This logic is not perfect as if flush happens after enqueue and before dequeue, that entry may never get
      // evicted (depends on when the next compaction happens, whether the queue configuration has been change or not).

      // There are two data columns, "d" and "m".
      // If the size == 2, it should not be evicted as well,
      // as state columns (dequeue) always happen after data columns (enqueue).
      if (result.size() <= 2) {
        return false;
      }

      // "d" and "m" columns always comes before the state columns, prefixed with "s".
      Iterator<KeyValue> iterator = result.iterator();
      if (!isColumn(iterator.next(), QueueConstants.DATA_COLUMN)) {
        return false;
      }
      if (!isColumn(iterator.next(), QueueConstants.META_COLUMN)) {
        return false;
      }

      // Need to determine if this row can be evicted iff all consumer groups have committed process this row.
      int consumedGroups = 0;
      // Inspect each state column
      while (iterator.hasNext()) {
        KeyValue kv = iterator.next();
        if (!isStateColumn(kv)) {
          continue;
        }
        // If any consumer has a state != PROCESSED, it should not be evicted
        if (!isProcessed(kv, consumerInstance)) {
          break;
        }
        // If it is PROCESSED, check if this row is smaller than the consumer instance startRow.
        // Essentially a loose check of committed PROCESSED.
        byte[] startRow = consumerConfig.getStartRow(consumerInstance);
        if (startRow != null && compareRowKey(kv, startRow) < 0) {
          consumedGroups++;
        }
      }

      // It can be evicted if from the state columns, it's been processed by all consumer groups
      // Otherwise, this row has to be less than smallest among all current consumers.
      // The second condition is for handling consumer being removed after it consumed some entries.
      // However, the second condition alone is not good enough as it's possible that in hash partitioning,
      // only one consumer is keep consuming when the other consumer never proceed.
      return consumedGroups == consumerConfig.getNumGroups()
          || compareRowKey(result.get(0), consumerConfig.getSmallestStartRow()) < 0;
    }

    /**
     * Returns {@code true} if the given {@link KeyValue} is a state column in queue entry row.
     */
    private boolean isStateColumn(KeyValue keyValue) {
      byte[] buffer = keyValue.getBuffer();

      int fCmp = Bytes.compareTo(QueueConstants.COLUMN_FAMILY, 0, QueueConstants.COLUMN_FAMILY.length,
                                   buffer, keyValue.getFamilyOffset(), keyValue.getFamilyLength());
      return fCmp == 0 && isPrefix(buffer, keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
                                   QueueConstants.STATE_COLUMN_PREFIX);
    }

    private boolean isColumn(KeyValue keyValue, byte[] qualifier) {
      byte[] buffer = keyValue.getBuffer();

      int fCmp = Bytes.compareTo(QueueConstants.COLUMN_FAMILY, 0, QueueConstants.COLUMN_FAMILY.length,
                                 buffer, keyValue.getFamilyOffset(), keyValue.getFamilyLength());
      return fCmp == 0 && (Bytes.compareTo(qualifier, 0, qualifier.length,
                                           buffer, keyValue.getQualifierOffset(), keyValue.getQualifierLength()) == 0);
    }

    private int compareRowKey(KeyValue kv, byte[] row) {
      return Bytes.compareTo(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(), row, 0, row.length);
    }

    /**
     * Returns {@code true} if the given {@link KeyValue} has a {@link ConsumerEntryState#PROCESSED} state and
     * also put the consumer information into the given {@link ConsumerInstance}.
     * Otherwise, returns {@code false} and the {@link ConsumerInstance} is left untouched.
     */
    private boolean isProcessed(KeyValue keyValue, ConsumerInstance consumerInstance) {
      byte[] buffer = keyValue.getBuffer();
      int stateIdx = keyValue.getValueOffset() + keyValue.getValueLength() - 1;
      boolean processed = buffer[stateIdx] == ConsumerEntryState.PROCESSED.getState();

      if (processed) {
        // Column is "s<groupId>"
        long groupId = Bytes.toLong(buffer, keyValue.getQualifierOffset() + 1);
        // Value is "<writePointer><instanceId><state>"
        int instanceId = Bytes.toInt(buffer, keyValue.getValueOffset() + LONG_BYTES);
        consumerInstance.setGroupInstance(groupId, instanceId);
      }
      return processed;
    }
  }
}
