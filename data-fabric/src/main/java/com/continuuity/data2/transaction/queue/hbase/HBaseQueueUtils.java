/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.weave.filesystem.Location;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class HBaseQueueUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseQueueUtils.class);

  /**
   * Creates a HBase queue table if the table doesn't exists.
   *
   * @param admin
   * @param tableName
   * @param maxWaitMs
   * @param coProcessorJar
   * @param coProcessors
   * @throws IOException
   */

  public static void createTableIfNotExists(HBaseAdmin admin, byte[] tableName,
                                            byte[] columnFamily, long maxWaitMs,
                                            int splits, Location coProcessorJar,
                                            String... coProcessors) throws IOException {
    // todo consolidate this method with HBaseTableUtil
    if (!admin.tableExists(tableName)) {
      HTableDescriptor htd = new HTableDescriptor(tableName);
      if (coProcessorJar != null) {
        for (String coProcessor : coProcessors) {
          htd.addCoprocessor(coProcessor, new Path(coProcessorJar.toURI()), Coprocessor.PRIORITY_USER, null);
        }
      }

      HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
      htd.addFamily(hcd);
      hcd.setMaxVersions(1);

      String tableNameString = Bytes.toString(tableName);
      byte[][] splitKeys = getSplitKeys(splits);

      try {
        LOG.info("Creating queue table '{}'", tableNameString);
        admin.createTable(htd, splitKeys);
        return;
      } catch (TableExistsException e) {
        // table may exist because someone else is creating it at the same
        // time. But it may not be available yet, and opening it might fail.
        LOG.info("Failed to create queue table '{}'. {}.", tableNameString, e.getMessage(), e);
      }

      // Wait for table to materialize
      try {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        while (stopwatch.elapsedTime(TimeUnit.MILLISECONDS) < maxWaitMs) {
          if (admin.tableExists(tableName)) {
            LOG.info("Table '{}' exists now. Assuming that another process concurrently created it.",
                     tableNameString);
            return;
          } else {
            TimeUnit.MILLISECONDS.sleep(100);
          }
        }
      } catch (InterruptedException e) {
        LOG.warn("Sleeping thread interrupted.");
      }
      LOG.error("Table '{}' does not exist after waiting {} ms. Giving up.", tableNameString, maxWaitMs);
    }
  }

  // For simplicity we allow max 255 splits for now
  private static final int MAX_SPLIT_COUNT = 0xff;

  static byte[][] getSplitKeys(int splits) {
    Preconditions.checkArgument(splits >= 1 && splits <= MAX_SPLIT_COUNT,
                                "Number of pre-splits should be in [1.." + MAX_SPLIT_COUNT + "] interval");

    if (splits == 1) {
      return new byte[0][];
    }

    int prefixesPerSplit = (MAX_SPLIT_COUNT + 1) / splits;

    // HBase will figure out first split to be started from beginning
    byte[][] splitKeys = new byte[splits - 1][];
    for (int i = 0; i < splits - 1; i++) {
      // "1 + ..." to make it a bit more fair
      int splitStartPrefix = (i + 1) * prefixesPerSplit;
      splitKeys[i] = new byte[] {(byte) splitStartPrefix};
    }

    return splitKeys;
  }

  /**
   * Returns the column qualifier for the consumer state column. The qualifier is formed by
   * {@code <groupId><instanceId>}.
   * @param groupId Group ID of the consumer
   * @param instanceId Instance ID of the consumer
   * @return A new byte[] which is the column qualifier.
   */
  public static byte[] getConsumerStateColumn(long groupId, int instanceId) {
    byte[] column = new byte[Longs.BYTES + Ints.BYTES];
    Bytes.putLong(column, 0, groupId);
    Bytes.putInt(column, Longs.BYTES, instanceId);
    return column;
  }
}
