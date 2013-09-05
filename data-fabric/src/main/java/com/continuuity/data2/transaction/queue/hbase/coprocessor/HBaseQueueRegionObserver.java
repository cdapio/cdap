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
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * RegionObserver for queue table. This class should only have JSE and HBase classes dependencies only.
 * It can also has dependencies on continuuity classes provided that all the transitive dependencies stay within
 * the mentioned scope.
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

        // If current queue is known and prefix match, then it's a queue entry, hence should be eligible.
        boolean eligible = (currentQueue != null
          && isPrefix(keyValue.getBuffer(), keyValue.getRowOffset() + 2, keyValue.getRowLength() - 2, currentQueue));

        if (!eligible) {
          // If not eligible, it could be scanned into the next queue entry or simply it hasn't see any queue entry yet.
          currentQueue = null;

          // Either case, it check if current row is a queue entry.
          eligible = isPrefix(keyValue.getBuffer(),
                              keyValue.getRowOffset() + 2,
                              keyValue.getRowLength() - 2,
                              QUEUE_BYTES);

          if (!eligible) {
            if (LOG.isDebugEnabled()) {
              String key = Bytes.toStringBinary(keyValue.getBuffer(),
                                                keyValue.getRowOffset(),
                                                keyValue.getRowLength());
              LOG.debug("Row " + key + " is not eligible for eviction.");
            }
            return hasNext;
          }
        }

        // This row is a queue entry. If currentQueue is null, meaning it's a new queue encountered during scan.
        if (currentQueue == null) {
          // Entry key is always (2 MD5 bytes + queueName + longWritePointer + intCounter)
          int queueNameEnd = keyValue.getRowOffset() + keyValue.getRowLength() - LONG_BYTES - INT_BYTES;
          currentQueue = Arrays.copyOfRange(keyValue.getBuffer(), keyValue.getRowOffset() + 2, queueNameEnd);
          consumerConfig = getConsumerConfig(currentQueue);
        }

        boolean evict;
        if (consumerConfig.getNumGroups() == 0) {
          // If no consumer group, this queue is dead, should be ok to evict.
          evict = true;
        } else if (consumerConfig.getNumGroups() < 0) {
          // If unknown consumer config (due to error), keep the queue.
          evict = false;
        } else {
          // Need to determine if this row can be evicted iff all consumer groups must have committed process this row.
          int consumedGroups = 0;
          // Inspect each state column
          for (KeyValue kv : result) {
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
            if (startRow != null && Bytes.compareTo(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(),
                                                    startRow, 0, startRow.length) < 0) {
              consumedGroups++;
            }
          }
          evict = consumedGroups == consumerConfig.getNumGroups();
        }

        if (evict) {
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
     * Returns {@code true} if the given {@link KeyValue} is a state column in queue entry row.
     */
    private boolean isStateColumn(KeyValue keyValue) {
      byte[] buffer = keyValue.getBuffer();

      int fCmp = Bytes.compareTo(QueueConstants.COLUMN_FAMILY, 0, QueueConstants.COLUMN_FAMILY.length,
                                   buffer, keyValue.getFamilyOffset(), keyValue.getFamilyLength());
      return fCmp == 0 && isPrefix(buffer, keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
                                   QueueConstants.STATE_COLUMN_PREFIX);
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
