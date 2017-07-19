/*
 * Copyright © 2014-2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.dataset2.lib.table.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.TableProperties;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.DeleteBuilder;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.PutBuilder;
import co.cask.cdap.data2.util.hbase.ScanBuilder;
import co.cask.cdap.hbase.wd.AbstractRowKeyDistributor;
import co.cask.cdap.hbase.wd.DistributedScanner;
import co.cask.cdap.hbase.wd.RowKeyDistributorByHashPrefix;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * An HBase metrics table client.
 */
public class HBaseMetricsTable implements MetricsTable {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseMetricsTable.class);
  // Exponentially log less on executor rejected execution due to limit threads
  private static final Logger REJECTION_LOG = Loggers.sampling(LOG, LogSamplers.exponentialLimit(1, 1024, 2.0d));

  private final HBaseTableUtil tableUtil;
  private final TableId tableId;
  private final HTable hTable;
  private final byte[] columnFamily;
  private final boolean isV3Table;
  private final AbstractRowKeyDistributor rowKeyDistributor;
  private final ExecutorService scanExecutor;

  public HBaseMetricsTable(DatasetContext datasetContext, DatasetSpecification spec,
                           Configuration hConf, HBaseTableUtil tableUtil) throws IOException {
    this.tableUtil = tableUtil;
    this.tableId = tableUtil.createHTableId(new NamespaceId(datasetContext.getNamespaceId()), spec.getName());
    this.isV3Table = Boolean.parseBoolean(spec.getProperty(Constants.Metrics.DEFAULT_METRIC_V3_TABLE_PREFIX, "false"));
    HTable hTable = tableUtil.createHTable(hConf, tableId);
    // todo: make configurable
    hTable.setWriteBufferSize(HBaseTableUtil.DEFAULT_WRITE_BUFFER_SIZE);
    hTable.setAutoFlushTo(false);
    this.hTable = hTable;
    this.columnFamily = TableProperties.getColumnFamilyBytes(spec.getProperties());

    RejectedExecutionHandler callerRunsPolicy = new RejectedExecutionHandler() {

      @Override
      public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        REJECTION_LOG.info(
          "No more threads in the HBase scan thread pool. Consider increase {}. Scan from caller thread {}",
          Constants.MessagingSystem.HBASE_MAX_SCAN_THREADS, Thread.currentThread().getName()
        );
        // Runs it from the caller thread
        if (!executor.isShutdown()) {
          r.run();
        }
      }
    };

    // Creates a executor that will shrink to 0 threads if left idle
    // Uses daemon thread, hence no need to worry about shutdown
    // When all threads are busy, use the caller thread to execute
    this.scanExecutor = new ThreadPoolExecutor(0, spec.getIntProperty(Constants.Metrics.METRICS_HBASE_MAX_SCAN_THREADS,
                                                                      96),
                                               60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
                                               Threads.createDaemonThreadFactory("metrics-hbase-scanner-%d"),
                                               callerRunsPolicy);

    rowKeyDistributor = getRowKeyDistributor(spec);
  }

  private AbstractRowKeyDistributor getRowKeyDistributor(DatasetSpecification spec) throws IOException {
    if (isV3Table) {
      try {
        String splits = spec.getProperty(Constants.Metrics.DEFAULT_METRIC_HBASE_TABLE_SPLITS);
        if (splits == null) {
          // Cannot be null
          throw new IOException("Missing table property " + Constants.Metrics.DEFAULT_METRIC_HBASE_TABLE_SPLITS +
                                  " on HBase table " + tableId);
        }
        int numOfsplits = Integer.parseInt(splits);
        return new RowKeyDistributorByHashPrefix(new RowKeyDistributorByHashPrefix.OneByteSimpleHash(numOfsplits));
      } catch (NumberFormatException e) {
        throw new IOException("Invalid value for property " + Constants.Metrics.DEFAULT_METRIC_HBASE_TABLE_SPLITS
                                + " on HBase table " + tableId, e);
      }
    }

    return null;
  }

  @Override
  @Nullable
  public byte[] get(byte[] row, byte[] column) {
    try {
      byte[] distributedKey = getRowKey(row);
      Get get = tableUtil.buildGet(distributedKey)
        .addColumn(columnFamily, column)
        .setMaxVersions(1)
        .build();
      Result getResult = hTable.get(get);
      if (!getResult.isEmpty()) {
        return getResult.getValue(columnFamily, column);
      }
      return null;
    } catch (IOException e) {
      throw new DataSetException("Get failed on table " + tableId, e);
    }
  }

  @Override
  public void put(SortedMap<byte[], ? extends SortedMap<byte[], Long>> updates) {
    List<Put> puts = Lists.newArrayList();
    for (Map.Entry<byte[], ? extends SortedMap<byte[], Long>> row : updates.entrySet()) {
      byte[] distributedKey = getRowKey(row.getKey());
      PutBuilder put = tableUtil.buildPut(distributedKey);
      for (Map.Entry<byte[], Long> column : row.getValue().entrySet()) {
        put.add(columnFamily, column.getKey(), Bytes.toBytes(column.getValue()));
      }
      puts.add(put.build());
    }
    try {
      hTable.put(puts);
      hTable.flushCommits();
    } catch (IOException e) {
      throw new DataSetException("Put failed on table " + tableId, e);
    }
  }

  @Override
  public void putBytes(SortedMap<byte[], ? extends SortedMap<byte[], byte[]>> updates) {
    List<Put> puts = Lists.newArrayList();
    for (Map.Entry<byte[], ? extends SortedMap<byte[], byte[]>> row : updates.entrySet()) {
      byte[] distributedKey = getRowKey(row.getKey());
      PutBuilder put = tableUtil.buildPut(distributedKey);
      for (Map.Entry<byte[], byte[]> column : row.getValue().entrySet()) {
        put.add(columnFamily, column.getKey(), column.getValue());
      }
      puts.add(put.build());
    }
    try {
      hTable.put(puts);
      hTable.flushCommits();
    } catch (IOException e) {
      throw new DataSetException("Put failed on table " + tableId, e);
    }
  }

  @Override
  public boolean swap(byte[] row, byte[] column, byte[] oldValue, byte[] newValue) {
    try {
      byte[] distributedKey = getRowKey(row);
      if (newValue == null) {
        // HBase API weirdness: we must use deleteColumns() because deleteColumn() deletes only the last version.
        Delete delete = tableUtil.buildDelete(distributedKey)
          .deleteColumns(columnFamily, column)
          .build();
        return hTable.checkAndDelete(distributedKey, columnFamily, column, oldValue, delete);
      } else {
        Put put = tableUtil.buildPut(distributedKey)
          .add(columnFamily, column, newValue)
          .build();
        return hTable.checkAndPut(distributedKey, columnFamily, column, oldValue, put);
      }
    } catch (IOException e) {
      throw new DataSetException("Swap failed on table " + tableId, e);
    }
  }

  @Override
  public void increment(byte[] row, Map<byte[], Long> increments) {
    byte[] distributedKey = getRowKey(row);
    Put increment = getIncrementalPut(distributedKey, increments);
    try {
      hTable.put(increment);
      hTable.flushCommits();
    } catch (IOException e) {
      // figure out whether this is an illegal increment
      // currently there is not other way to extract that from the HBase exception than string match
      if (e.getMessage() != null && e.getMessage().contains("isn't 64 bits wide")) {
        throw new NumberFormatException("Attempted to increment a value that is not convertible to long," +
                                          " row: " + Bytes.toStringBinary(distributedKey));
      }
      throw new DataSetException("Increment failed on table " + tableId, e);
    }
  }

  private byte[] getRowKey(byte[] row) {
    return isV3Table ? rowKeyDistributor.getDistributedKey(row) : row;
  }

  private Put getIncrementalPut(byte[] row, Map<byte[], Long> increments) {
    Put increment = getIncrementalPut(row);
    for (Map.Entry<byte[], Long> column : increments.entrySet()) {
      // note: we use default timestamp (current), which is fine because we know we collect metrics no more
      //       frequent than each second. We also rely on same metric value to be processed by same metric processor
      //       instance, so no conflicts are possible.
      increment.add(columnFamily, column.getKey(), Bytes.toBytes(column.getValue()));
    }
    return increment;
  }

  private Put getIncrementalPut(byte[] row) {
    return tableUtil.buildPut(row)
      .setAttribute(HBaseTable.DELTA_WRITE, Bytes.toBytes(true))
      .build();
  }

  @Override
  public void increment(NavigableMap<byte[], NavigableMap<byte[], Long>> updates) {
    List<Put> puts = Lists.newArrayList();
    for (Map.Entry<byte[], NavigableMap<byte[], Long>> row : updates.entrySet()) {
      byte[] distributedKey = getRowKey(row.getKey());
      Put increment = getIncrementalPut(distributedKey, row.getValue());
      puts.add(increment);
    }

    try {
      hTable.put(puts);
      hTable.flushCommits();
    } catch (IOException e) {
      // figure out whether this is an illegal increment
      // currently there is not other way to extract that from the HBase exception than string match
      if (e.getMessage() != null && e.getMessage().contains("isn't 64 bits wide")) {
        throw new NumberFormatException("Attempted to increment a value that is not convertible to long.");
      }
      throw new DataSetException("Increment failed on table " + tableId, e);
    }
  }

  @Override
  public long incrementAndGet(byte[] row, byte[] column, long delta) {
    byte[] distributedKey = getRowKey(row);
    Increment increment = new Increment(distributedKey);
    increment.addColumn(columnFamily, column, delta);
    try {
      Result result = hTable.increment(increment);
      return Bytes.toLong(result.getValue(columnFamily, column));
    } catch (IOException e) {
      // figure out whether this is an illegal increment
      // currently there is not other way to extract that from the HBase exception than string match
      if (e.getMessage() != null && e.getMessage().contains("isn't 64 bits wide")) {
        throw new NumberFormatException("Attempted to increment a value that is not convertible to long," +
                                          " row: " + Bytes.toStringBinary(distributedKey) +
                                          " column: " + Bytes.toStringBinary(column));
      }
      throw new DataSetException("IncrementAndGet failed on table " + tableId, e);
    }
  }

  @Override
  public void delete(byte[] row, byte[][] columns) {
    byte[] distributedKey = getRowKey(row);
    DeleteBuilder delete = tableUtil.buildDelete(distributedKey);
    for (byte[] column : columns) {
      delete.deleteColumns(columnFamily, column);
    }
    try {
      hTable.delete(delete.build());
    } catch (IOException e) {
      throw new DataSetException("Delete failed on table " + tableId, e);
    }
  }

  @Override
  public Scanner scan(@Nullable byte[] startRow, @Nullable byte[] stopRow,
                      @Nullable FuzzyRowFilter filter) {
    ScanBuilder scanBuilder = tableUtil.buildScan();
    configureRangeScan(scanBuilder, startRow, stopRow, filter);
    try {
      ResultScanner resultScanner;
      if (isV3Table) {
        resultScanner = DistributedScanner.create(hTable, scanBuilder.build(), rowKeyDistributor,
                                                    scanExecutor);
      } else {
        resultScanner = hTable.getScanner(scanBuilder.build());
      }

      return new HBaseScanner(resultScanner, columnFamily);
    } catch (IOException e) {
      throw new DataSetException("Scan failed on table " + tableId, e);
    }
  }

  private ScanBuilder configureRangeScan(ScanBuilder scan, @Nullable byte[] startRow, @Nullable byte[] stopRow,
                                         @Nullable FuzzyRowFilter filter) {
    // todo: should be configurable
    scan.setCaching(1000);

    if (startRow != null) {
      scan.setStartRow(startRow);
    }
    if (stopRow != null) {
      scan.setStopRow(stopRow);
    }
    scan.addFamily(columnFamily);
    if (filter != null) {
      List<Pair<byte[], byte[]>> fuzzyPairs = Lists.newArrayListWithExpectedSize(filter.getFuzzyKeysData().size());
      for (ImmutablePair<byte[], byte[]> pair : filter.getFuzzyKeysData()) {
        fuzzyPairs.add(Pair.newPair(pair.getFirst(), pair.getSecond()));
      }
      scan.setFilter(new org.apache.hadoop.hbase.filter.FuzzyRowFilter(fuzzyPairs));
    }
    return scan;
  }

  @Override
  public void close() throws IOException {
    hTable.close();
  }
}
