/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.api.common.Bytes;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
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
    private byte[] currentQueue;
    private byte[] smallestRowKey;

    private EvictionInternalScanner(RegionCoprocessorEnvironment env, InternalScanner scanner) {
      this.env = env;
      this.scanner = scanner;
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
          smallestRowKey = getSmallestRowKey(currentQueue);
        }

        // If it is smaller than the smallest row, then skip this row and keep scanning.
        // Since there is already comparison above to test the queue prefix, we can skip the queue name comparison here.
        if (Bytes.compareTo(smallestRowKey,
                            currentQueue.length + 2,
                            smallestRowKey.length - currentQueue.length - 2,
                            keyValue.getBuffer(),
                            keyValue.getRowOffset() + currentQueue.length + 2,
                            keyValue.getRowLength() - currentQueue.length - 2) >= 0) {
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

    private byte[] getSmallestRowKey(byte[] queueName) {
      try {
        // Fetch the queue consumers information
        HTableInterface hTable = env.getTable(env.getRegion().getTableDesc().getName());
        try {
          Get get = new Get(queueName);
          get.addFamily(QueueConstants.COLUMN_FAMILY);

          Result result = hTable.get(get);

          if (result == null || result.isEmpty()) {
            return Bytes.EMPTY_BYTE_ARRAY;
          }

          NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(QueueConstants.COLUMN_FAMILY);
          if (familyMap == null) {
            return Bytes.EMPTY_BYTE_ARRAY;
          }

          byte[] smallest = null;
          for (byte[] value : familyMap.values()) {
            if (smallest == null || Bytes.BYTES_COMPARATOR.compare(value, smallest) < 0) {
              smallest = value;
            }
          }

          return smallest;
        } finally {
          hTable.close();
        }
      } catch (IOException e) {
        // If there is exception when fetching consumers information, return EMPTY_BYTE_ARRAY, meaning keep the row.
        LOG.error("Exception when looking up smallest row key for " + Bytes.toStringBinary(queueName), e);
        return Bytes.EMPTY_BYTE_ARRAY;
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
  }
}
