/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.data2.transaction.queue.ConsumerEntryState;
import com.continuuity.data2.transaction.queue.QueueConstants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * A CoProcessor endpoint that do eviction on Queue table.
 */
public final class HBaseQueueEvictionEndpoint extends BaseEndpointCoprocessor implements HBaseQueueEvictionProtocol {

  private static final Log LOG = LogFactory.getLog(HBaseQueueEvictionEndpoint.class);

  // Some reasonable size for collection rows to delete to avoid too frequent overhead of resizing the array.
  private static final int COLLECTION_SIZE = 1000;

  private static final int LONG_SIZE = Long.SIZE / Byte.SIZE;
  private static final int INT_SIZE = Integer.SIZE / Byte.SIZE;

  private final Filter stateColumnFilter = new ColumnPrefixFilter(QueueConstants.STATE_COLUMN_PREFIX);
  private final boolean supportFastScan;

  public HBaseQueueEvictionEndpoint() {
    boolean supportFastScan;
    try {
      RegionScanner.class.getMethod("nextRaw", List.class, String.class);
      supportFastScan = true;
    } catch (NoSuchMethodException e) {
      LOG.warn("RegionScanner.nextRaw is not supported in this HBase version.");
      supportFastScan = false;
    }
    this.supportFastScan = supportFastScan;
  }

  @Override
  public int evict(Scan scan, long readPointer, long[] excludes, int numGroups) throws IOException {
    CoprocessorEnvironment env = getEnvironment();
    if (!(env instanceof RegionCoprocessorEnvironment)) {
      LOG.warn("Environment is not RegionCoprocessorEnvironment. Ignore request.");
      return 0;
    }

    LOG.debug(String.format("Evict request received: start=%s, readPointer=%d, exclude size=%d, numGroups=%d",
                           scan.getStartRow(), readPointer, excludes.length, numGroups));

    HRegion region = ((RegionCoprocessorEnvironment) env).getRegion();

    // Setup scan filter to gives state columns only.
    Filter filter = scan.getFilter();
    if (filter == null) {
      filter = stateColumnFilter;
    } else {
      filter = new FilterList(stateColumnFilter, filter);
    }
    scan.setFilter(filter);
    scan.setBatch(numGroups);

    // Scan and delete rows
    RegionScanner scanner = region.getScanner(scan);
    List<Pair<Mutation, Integer>> deletes = new ArrayList<Pair<Mutation, Integer>>(COLLECTION_SIZE);

    if (supportFastScan) {
      fastScanForDeletes(region, scanner, readPointer, excludes, numGroups, deletes);
    } else {
      scanForDeletes(scanner, readPointer, excludes, numGroups, deletes);
    }

    if (deletes.isEmpty()) {
      LOG.debug(String.format("No entry to evict from region %s", region));
      return 0;
    }

    region.batchMutate(deletes.toArray(new Pair[deletes.size()]));

    LOG.debug(String.format("Evicted %d entries from region %s", deletes.size(), region));

    return deletes.size();
  }

  private void scanForDeletes(RegionScanner scanner,
                              long readPointer, long[] excludes, int numGroups,
                              List<Pair<Mutation, Integer>> deletes) throws IOException {
    List<KeyValue> keyValues = new ArrayList<KeyValue>();
    boolean hasMore = true;
    try {
      while (hasMore) {
        keyValues.clear();
        hasMore = scanner.next(keyValues);
        processKeyValues(keyValues, deletes, readPointer, excludes, numGroups);
      }
    } finally {
      scanner.close();
    }
  }

  private void fastScanForDeletes(HRegion region, RegionScanner scanner,
                                  long readPointer, long[] excludes, int numGroups,
                                  List<Pair<Mutation, Integer>> deletes) throws IOException {
    List<KeyValue> keyValues = new ArrayList<KeyValue>();
    boolean hasMore = true;
    MultiVersionConsistencyControl.setThreadReadPoint(scanner.getMvccReadPoint());
    region.startRegionOperation();
    try {
      synchronized (scanner) {
        while (hasMore) {
          keyValues.clear();
          hasMore = scanner.nextRaw(keyValues, null);
          processKeyValues(keyValues, deletes, readPointer, excludes, numGroups);
        }
      }
    } finally {
      scanner.close();
      region.closeRegionOperation();
    }
  }

  private void processKeyValues(List<KeyValue> keyValues, List<Pair<Mutation, Integer>> deletes,
                                long readPointer, long[] excludes, int numGroups) {
    // Count how many committed processed of a row
    int committedProcess = 0;
    KeyValue keyValue = null;
    Iterator<KeyValue> iterator = keyValues.iterator();
    while (iterator.hasNext()) {
      keyValue = iterator.next();
      if (isCommittedProcessed(keyValue, readPointer, excludes)) {
        committedProcess++;
      }
    }
    if (keyValue != null && committedProcess == numGroups) {
      deletes.add(Pair.<Mutation, Integer>newPair(new Delete(keyValue.getRow()), null));
    }
  }

  private boolean isCommittedProcessed(KeyValue stateColumn, long readPointer, long[] excludes) {
    long writePointer = Bytes.toLong(stateColumn.getBuffer(), stateColumn.getValueOffset(), LONG_SIZE);
    if (writePointer > readPointer || Arrays.binarySearch(excludes, writePointer) >= 0) {
      return false;
    }
    byte state = stateColumn.getBuffer()[stateColumn.getValueOffset() + LONG_SIZE + INT_SIZE];
    return state == ConsumerEntryState.PROCESSED.getState();
  }
}
