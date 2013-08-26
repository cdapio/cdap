/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.api.common.Bytes;
import com.google.common.base.Stopwatch;
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
   * @param admin
   * @param tableName
   * @param maxWaitMs
   * @param coProcessorJar
   * @param coProcessors
   * @throws IOException
   */
  public static void createTableIfNotExists(HBaseAdmin admin, byte[] tableName,
                                            byte[] columnFamily, long maxWaitMs,
                                            Path coProcessorJar, String...coProcessors) throws IOException {
    if (!admin.tableExists(tableName)) {
      HTableDescriptor htd = new HTableDescriptor(tableName);
      if (coProcessorJar != null) {
        for (String coProcessor : coProcessors) {
          htd.addCoprocessor(coProcessor, coProcessorJar, Coprocessor.PRIORITY_USER, null);
        }
      }

      HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
      htd.addFamily(hcd);
      hcd.setMaxVersions(1);
      String tableNameString = Bytes.toString(tableName);

      try {
        LOG.info("Creating queue table '{}'", tableNameString);
        admin.createTable(htd);
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
}
