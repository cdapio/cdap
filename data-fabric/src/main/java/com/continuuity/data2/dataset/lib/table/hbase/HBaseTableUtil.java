package com.continuuity.data2.dataset.lib.table.hbase;

import com.google.common.base.Stopwatch;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.concurrent.TimeUnit;

/**
 * Common utilities for dealing with HBase.
 */
public class HBaseTableUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseTableUtil.class);

  public static final long MAX_CREATE_TABLE_WAIT = 5000L;    // Maximum wait of 5 seconds for table creation.

  public static String getHBaseTableName(String tableName) {
    return encodeTableName(tableName);
  }

  private static String encodeTableName(String tableName) {
    try {
      return URLEncoder.encode(tableName, "ASCII");
    } catch (UnsupportedEncodingException e) {
      // this can never happen - we know that ASCII is a supported character set!
      LOG.error("Error encoding table name '" + tableName + "'", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Create a hbase table if it does not exist. Deals with race conditions when two clients concurrently attempt to
   * create the table.
   * @param admin the hbase admin
   * @param tableName the name of the table
   * @param columnFamily the column family for data
   */
  public static void createTableIfNotExists(HBaseAdmin admin, String tableName,
                                            byte[] columnFamily) throws IOException {
    if (!admin.tableExists(tableName)) {

      HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);
      // todo: make stuff configurable
      // todo: using snappy compression for some reason breaks mini-hbase cluster (i.e. unit-test doesn't work)
      //    columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
      columnDescriptor.setMaxVersions(100);
      columnDescriptor.setBloomFilterType(StoreFile.BloomType.ROW);

      HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
      tableDescriptor.addFamily(columnDescriptor);

      try {
        LOG.info("Creating table '{}'", tableName);
        admin.createTable(tableDescriptor);
        return;
      } catch (TableExistsException e) {
        // table may exist because someone else is creating it at the same
        // time. But it may not be available yet, and opening it might fail.
        LOG.info("Failed to create queue table '{}'. {}.", tableName, e.getMessage(), e);
      }

      // Wait for table to materialize
      try {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        while (stopwatch.elapsedTime(TimeUnit.MILLISECONDS) < MAX_CREATE_TABLE_WAIT) {
          if (admin.tableExists(tableName)) {
            LOG.info("Table '{}' exists now. Assuming that another process concurrently created it.", tableName);
            return;
          } else {
            TimeUnit.MILLISECONDS.sleep(100);
          }
        }
      } catch (InterruptedException e) {
        LOG.warn("Sleeping thread interrupted.");
      }
      LOG.error("Table '{}' does not exist after waiting {} ms. Giving up.", tableName, MAX_CREATE_TABLE_WAIT);
    }
  }

}
