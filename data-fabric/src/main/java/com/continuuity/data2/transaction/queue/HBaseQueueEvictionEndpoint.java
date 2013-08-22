/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public final class HBaseQueueEvictionEndpoint extends BaseEndpointCoprocessor implements HBaseQueueEvictionProtocol {

  private static final Log LOG = LogFactory.getLog(HBaseQueueEvictionEndpoint.class);
  private static final byte PROCESSED = 2;

  @Override
  public int evict(Scan scan, long readPointer, long smallestExclude, int numGroups) throws IOException {
    CoprocessorEnvironment env = getEnvironment();
    if (!(env instanceof RegionCoprocessorEnvironment)) {
      LOG.warn("Environment is not RegionCoprocessorEnvironment. Ignore request.");
      return 0;
    }

    LOG.info(String.format("Evict request received: readPointer=%d, smallestExclude=%d, numGroups=%d",
                           readPointer, smallestExclude, numGroups));

    // Scan this region and deletes rows that have numGroups of state columns and all have committed process state
    List<KeyValue> keyValues = new ArrayList<KeyValue>();
    boolean hasMore = true;
    byte[] currentRow = null;
    // Track how many committed processed state of the current row
    int committedProcess = 0;
    List<Pair<Mutation, Integer>> deletes = new ArrayList<Pair<Mutation, Integer>>();

    HRegion region = ((RegionCoprocessorEnvironment) env).getRegion();
    RegionScanner scanner = region.getScanner(scan);
    try {
      while (hasMore) {
        keyValues.clear();
        hasMore = scanner.next(keyValues);

        // Collect all the state columns of a row
        for (KeyValue keyValue : keyValues) {
          byte[] row = keyValue.getRow();
          if (currentRow == null || Bytes.BYTES_COMPARATOR.compare(currentRow, row) != 0) {
            if (currentRow != null && committedProcess == numGroups) {
              // Delete it
              deletes.add(Pair.<Mutation, Integer>newPair(new Delete(currentRow), null));
            }
            currentRow = row;
            committedProcess = 0;
          }

          if (isCommittedProcessed(keyValue, readPointer, smallestExclude)) {
            committedProcess++;
          }
        }
      }
    } finally {
      scanner.close();
    }

    if (currentRow != null && committedProcess == numGroups) {
      deletes.add(Pair.<Mutation, Integer>newPair(new Delete(currentRow), null));
    }

    if (deletes.isEmpty()) {
      LOG.info(String.format("No entry to evict from region %s", region));
      return 0;
    }

    region.batchMutate(deletes.toArray(new Pair[0]));

    LOG.info(String.format("Evicted %d entries from region %s", deletes.size(), region));

    return deletes.size();
  }

  private boolean isCommittedProcessed(KeyValue stateColumn, long readPointer, long smallestExclude) {
    long writePointer = Bytes.toLong(stateColumn.getBuffer(), stateColumn.getValueOffset(), Longs.BYTES);
    if (writePointer > readPointer || writePointer >= smallestExclude) {
      return false;
    }
    return stateColumn.getBuffer()[stateColumn.getValueOffset() + Longs.BYTES + Ints.BYTES] == PROCESSED;
  }
}
