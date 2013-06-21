package com.continuuity.data.engine.hbase;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.table.AbstractOVCTable;
import com.continuuity.data.table.Scanner;
import com.continuuity.weave.common.Threads;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This implements the OVCTable for HBase.
 */
public class HBaseOVCTable extends AbstractOVCTable {
  private static final Logger Log = LoggerFactory.getLogger(HBaseOVCTable.class);

  //we should use enums :)
  static final byte DATA = (byte) 0x00; // regular data
  static final byte DELETE_VERSION = (byte) 0x01; // delete of a specific version
  static final byte DELETE_ALL = (byte) 0x02; // delete of all older versions of a column
  static final byte[] DELETE_VERSION_VALUE = new byte[]{DELETE_VERSION};
  static final byte[] DELETE_ALL_VALUE = new byte[]{DELETE_ALL};

  protected final HTable readTable;
  protected final LinkedList<HTable> writeTables;
  protected final Configuration conf;
  protected final byte[] tableName;
  protected final byte[] family;

  private final int maxPutsPerRpc;
  private final ExecutorService executorService;

  protected final IOExceptionHandler exceptionHandler;

  public HBaseOVCTable(CConfiguration cConf,
                       Configuration conf,
                       final byte[] tableName,
                       final byte[] family,
                       IOExceptionHandler exceptionHandler)
    throws OperationException {
    try {
      this.readTable = new HTable(conf, tableName);
      this.writeTables = new LinkedList<HTable>();
      this.writeTables.add(new HTable(conf, tableName));
      this.writeTables.add(new HTable(conf, tableName));
      this.conf = conf;
      this.tableName = tableName;
      this.family = family;
      this.exceptionHandler = exceptionHandler;
      this.maxPutsPerRpc = cConf.getInt(Constants.CFG_DATA_HBASE_PUTS_BATCH_MAX_SIZE,
                                        Constants.DEFAULT_DATA_HBASE_PUTS_BATCH_MAX_SIZE);
      int writeThreads = cConf.getInt(Constants.CFG_DATA_HBASE_TABLE_WRITE_THREADS_MAX_COUNT,
                                      Constants.DEFAULT_DATA_HBASE_TABLE_WRITE_THREADS_MAX_COUNT);

      // Thread pool of size max TX_EXECUTOR_POOL_SIZE.
      // 60 seconds wait time before killing idle threads.
      // Keep no idle threads more than 60 seconds.
      // If max thread pool size reached, execute the task in the submitter thread
      // (basically fallback to sync mode if things comes too fast
      this.executorService = new ThreadPoolExecutor(0, writeThreads,
                                                    60L, TimeUnit.SECONDS,
                                                    new SynchronousQueue<Runnable>(),
                                                    Threads.createDaemonThreadFactory("hbaseovct-async-write-executor"),
                                                    new ThreadPoolExecutor.CallerRunsPolicy());

    } catch (IOException e) {
      exceptionHandler.handle(e);
      //Note: exceptionHandler.handle already throws a RuntimeException. However IntelliJ doesn't recognize it and
      //marks all private members as un-initialized if the the throw statement below is commented out.
      throw new RuntimeException("this point should never be reached.");
    }
  }

  private synchronized HTable getWriteTable() throws IOException {
    HTable writeTable = this.writeTables.pollFirst();
    return writeTable == null ? new HTable(this.conf, this.tableName) : writeTable;
  }

  private synchronized void returnWriteTable(HTable table) {
    this.writeTables.add(table);
  }

  private byte[] prependWithTypePrefix(byte typePrefix, final byte[] value) {
    byte[] newValue = new byte[value.length + 1];
    System.arraycopy(value, 0, newValue, 1, value.length);
    newValue[0] = typePrefix;
    return newValue;
  }

  private byte[] removeTypePrefix(final byte[] value) {
    return Arrays.copyOfRange(value, 1, value.length);
  }

  @Override
  public void put(byte[] row, byte[][] columns, long version, byte[][] values) throws OperationException {
    assert (columns.length == values.length);
    HTable writeTable = null;
    try {
      writeTable = getWriteTable();
      Put put = new Put(row);
      for (int i = 0; i < columns.length; i++) {
        put.add(this.family, columns[i], version, prependWithTypePrefix(DATA, values[i]));
      }
      writeTable.put(put);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    } finally {
      if (writeTable != null) {
        returnWriteTable(writeTable);
      }
    }
  }

  @Override
  public void put(byte[][] rows, byte[][] columns, long version, byte[][] values) throws OperationException {
    assert (rows.length == columns.length);
    assert (columns.length == values.length);
    HTable writeTable = null;
    try {
      writeTable = getWriteTable();
      List<Put> puts = new ArrayList<Put>(rows.length);
      for (int i = 0; i < rows.length; i++) {
        Put put = new Put(rows[i]);
        put.add(this.family, columns[i], version, prependWithTypePrefix(DATA, values[i]));
        puts.add(put);
      }
      writeTable.put(puts);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    } finally {
      if (writeTable != null) {
        returnWriteTable(writeTable);
      }
    }
  }

  @Override
  public void put(byte[][] rows, byte[][][] columnsPerRow, final long version, byte[][][] valuesPerRow)
    throws OperationException {
    if (rows.length < maxPutsPerRpc) {
      putInternal(rows, columnsPerRow, version, valuesPerRow);
      return;
    }
    List<Future<?>> results = Lists.newArrayList();
    for (int j = 0; j < 1 + rows.length / maxPutsPerRpc; j++) {
      int count = (j + 1) * maxPutsPerRpc > rows.length ? (rows.length - j * maxPutsPerRpc) : maxPutsPerRpc;

      // TODO: avoid copying, see ENG-2826
      final byte[][] rowPart = Arrays.copyOfRange(rows, j * maxPutsPerRpc, j * maxPutsPerRpc + count);
      final byte[][][] columnsPart = Arrays.copyOfRange(columnsPerRow, j * maxPutsPerRpc, j * maxPutsPerRpc + count);
      final byte[][][] valuesPart = Arrays.copyOfRange(valuesPerRow, j * maxPutsPerRpc, j * maxPutsPerRpc + count);

      Future<?> result = executorService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            putInternal(rowPart, columnsPart, version, valuesPart);
          } catch (OperationException e) {
            throw new RuntimeException(e);
          }
        }
      });
      results.add(result);
    }

    OperationException operationException = null;
    for (Future<?> result : results) {
      try {
        result.get();
      } catch (Exception e) {
        // we do not re-throw it right away as we want to wait for all tasks to finish before we return from method,
        // i.e. before there will be any action taken on that. Like in case of rollback attempt, we want to do deletes
        // only after writes are finished, otherwise they may not have affect.
        operationException = new OperationException(StatusCode.INTERNAL_ERROR, "Writing puts failed", e);
        Log.warn("Writing puts failed", e);
      }
    }

    if (operationException != null) {
      throw operationException;
    }
  }

  private void putInternal(byte[][] rows, byte[][][] columnsPerRow, long version, byte[][][] valuesPerRow)
    throws OperationException {
    assert (rows.length == columnsPerRow.length);
    assert (rows.length == valuesPerRow.length);
    HTable writeTable = null;
    try {
      writeTable = getWriteTable();
      List<Put> puts = new ArrayList<Put>(rows.length);
      for (int i = 0; i < rows.length; i++) {
        byte[][] columns = columnsPerRow[i];
        byte[][] values = valuesPerRow[i];
        Put put = new Put(rows[i]);
        for (int j = 0; j < columns.length; j++) {
          put.add(this.family, columns[j], version, prependWithTypePrefix(DATA, values[j]));
        }
        puts.add(put);
      }
      writeTable.put(puts);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    } finally {
      if (writeTable != null) {
        returnWriteTable(writeTable);
      }
    }
  }

  @Override
  public void delete(byte[] row, byte[][] columns, long version) throws OperationException {
    HTable writeTable = null;
    //point delete (unlike deleteAll) is only used internally for undo-ing operations of i.e. write(?) or delete
    //by deleting deletes
    try {
      writeTable = getWriteTable();
      Put delPut = new Put(row);
      //adding tombstone to value of cell
      for (byte[] column : columns) {
        //adding tombstone to value of cell
        delPut.add(this.family, column, version, DELETE_VERSION_VALUE);
      }
      writeTable.put(delPut);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    } finally {
      if (writeTable != null) {
        returnWriteTable(writeTable);
      }
    }
  }

  @Override
  public void deleteAll(byte[] row, byte[][] columns, long version) throws OperationException {
    HTable writeTable = null;
    try {
      writeTable = getWriteTable();
      Put delPut = new Put(row);
      for (byte[] column : columns) {
        delPut.add(this.family, column, version, DELETE_ALL_VALUE);
      }
      writeTable.put(delPut);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    } finally {
      if (writeTable != null) {
        returnWriteTable(writeTable);
      }
    }
  }

  @Override
  public void undeleteAll(byte[] row, byte[][] columns, long version) throws OperationException {
    delete(row, columns, version);
  }

  @Override
  public void put(byte[] row, byte[] column, long version, byte[] value) throws OperationException {
    put(row, new byte[][]{column}, version, new byte[][]{value});
  }

  @Override
  public void delete(byte[] row, byte[] column, long version) throws OperationException {
    delete(row, new byte[][]{column}, version);
  }

  @Override
  public void deleteAll(byte[] row, byte[] column, long version) throws OperationException {
    deleteAll(row, new byte[][]{column}, version);
  }

  @Override
  public void deleteDirty(byte[] row, byte[][] columns, long version) throws OperationException {
    deleteAll(row, columns, version);
  }

  @Override
  public void deleteDirty(byte[][] rows) throws OperationException {
    // TODO: When a put dirty is done after deleteDirty, the put does not stick
    HTable writeTable = null;
    try {
      writeTable = getWriteTable();
      List<Delete> deletes = new ArrayList<Delete>(rows.length);
      for (byte[] row : rows) {
        Delete delete = new Delete(row);
        deletes.add(delete);
      }
      writeTable.delete(deletes);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    } finally {
      if (writeTable != null) {
        returnWriteTable(writeTable);
      }
    }
  }

  @Override
  public void undeleteAll(byte[] row, byte[] column, long version) throws OperationException {
    undeleteAll(row, new byte[][]{column}, version);
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> get(byte[] row, ReadPointer readPointer) throws OperationException {
    Set<Long> deleted = Sets.newHashSet();
    try {
      Get get = new Get(row);
      get.addFamily(this.family);
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();
      Result result = this.readTable.get(get);
      Map<byte[], byte[]> map = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
      boolean fastForwardToNextCol = false;
      byte[] previousColumn = null;
      //assumption: result.raw() has elements sorted by column (all cells from same column before next column)
      for (KeyValue kv : result.raw()) {
        byte[] column = kv.getQualifier();
        if (Bytes.equals(previousColumn, column) && fastForwardToNextCol) {
          continue;
        }
        fastForwardToNextCol = false;
        if (!Bytes.equals(previousColumn, column)) {
          deleted.clear();
        }
        long version = kv.getTimestamp();
        if (!readPointer.isVisible(version)) {
          continue;
        }
        if (deleted.contains(version)) {
          deleted.remove(version);
          continue;
        }
        byte[] value = kv.getValue();
        byte typePrefix = value[0];
        if (typePrefix == DATA) {
          byte[] trueValue = removeTypePrefix(value);
          map.put(column, trueValue);
          fastForwardToNextCol = true;
          deleted.clear(); // necessary?
        }
        if (typePrefix == DELETE_ALL) {
          fastForwardToNextCol = true;
          deleted.clear();
        }
        if (typePrefix == DELETE_VERSION) {
          deleted.add(version);
        }
        previousColumn = column;
      }

      if (map.isEmpty()) {
        return new OperationResult<Map<byte[], byte[]>>(StatusCode.KEY_NOT_FOUND);
      }
      return new OperationResult<Map<byte[], byte[]>>(map);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
    // as fall-back return "not found".
    return new OperationResult<Map<byte[], byte[]>>(StatusCode.COLUMN_NOT_FOUND);
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> get(byte[] row, byte[][] columns, ReadPointer readPointer)
    throws OperationException {
    try {
      Get get = new Get(row);
      for (byte[] column : columns) {
        get.addColumn(this.family, column);
      }
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();
      Result result = this.readTable.get(get);
      Map<byte[], byte[]> map = parseRowResult(result, readPointer);
      if (map.isEmpty()) {
        return new OperationResult<Map<byte[], byte[]>>(StatusCode.COLUMN_NOT_FOUND);
      } else {
        return new OperationResult<Map<byte[], byte[]>>(map);
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
    // as fall-back return "not found".
    return new OperationResult<Map<byte[], byte[]>>(StatusCode.COLUMN_NOT_FOUND);
  }

  private Map<byte[], byte[]> parseRowResult(Result result, ReadPointer readPointer) {
    Set<Long> deleted = Sets.newHashSet();
    Map<byte[], byte[]> map = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    byte[] last = null;
    for (KeyValue kv : result.raw()) {
      byte[] column = kv.getQualifier();
      long version = kv.getTimestamp();
      if (!readPointer.isVisible(version)) {
        continue;
      }
      if (deleted.contains(version)) {
        deleted.remove(version);
        continue;
      }
      if (Bytes.equals(last, column)) {
        continue;
      }
      byte[] value = kv.getValue();
      byte typePrefix = value[0];
      if (typePrefix == DATA) {
        map.put(column, removeTypePrefix(value));
        deleted.clear();
        last = column;
      }
      if (typePrefix == DELETE_ALL) {
        deleted.clear();
        last = column;
      }
      if (typePrefix == DELETE_VERSION) {
        deleted.add(version);
      }
    }
    return map;
  }

  @Override
  public OperationResult<Map<byte[], Map<byte[], byte[]>>> getAllColumns(byte[][] rows, byte[][] columns,
                                                                         ReadPointer readPointer)
    throws OperationException {
    // TODO: this can probably be improved by doing a scan instead of get.
    try {
      List<Get> gets = new ArrayList<Get>(rows.length);
      for (byte[] row : rows) {
        Get get = new Get(row);
        for (byte[] column : columns) {
          get.addColumn(this.family, column);
        }
        get.setTimeRange(0, getMaxStamp(readPointer));
        get.setMaxVersions();
        gets.add(get);
      }
      Result results[] = this.readTable.get(gets);
      Map<byte[], Map<byte[], byte[]>> resultMap = new TreeMap<byte[], Map<byte[], byte[]>>(Bytes.BYTES_COMPARATOR);
      for (Result result : results) {
        if (!result.isEmpty()) {
          Map<byte[], byte[]> map = parseRowResult(result, readPointer);
          resultMap.put(result.getRow(), map);
        }
      }
      if (resultMap.isEmpty()) {
        return new OperationResult<Map<byte[], Map<byte[], byte[]>>>(StatusCode.KEY_NOT_FOUND);
      } else {
        return new OperationResult<Map<byte[], Map<byte[], byte[]>>>(resultMap);
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
    // as fall-back return "not found".
    return new OperationResult<Map<byte[], Map<byte[], byte[]>>>(StatusCode.COLUMN_NOT_FOUND);
  }

  @Override
  public OperationResult<byte[]> get(byte[] row, byte[] column, ReadPointer readPointer) throws OperationException {
    Set<Long> deleted = Sets.newHashSet();
    try {
      Get get = new Get(row);
      get.addColumn(this.family, column);
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();
      Result result = this.readTable.get(get);
      for (KeyValue kv : result.raw()) {
        long version = kv.getTimestamp();
        if (!readPointer.isVisible(version) || deleted.contains(version)) {
          continue;
        }
        byte[] value = kv.getValue();
        if (value == null || value.length == 0) {
          return new OperationResult<byte[]>(StatusCode.COLUMN_NOT_FOUND);
        }
        byte typePrefix = value[0];
        switch (typePrefix) {
          case DATA:
            return new OperationResult<byte[]>(removeTypePrefix(value));
          case DELETE_VERSION:
            deleted.add(version);
            break;
          case DELETE_ALL:
            return new OperationResult<byte[]>(StatusCode.COLUMN_NOT_FOUND);
        }
      }
      return new OperationResult<byte[]>(StatusCode.COLUMN_NOT_FOUND);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
    // as fall-back return "not found".
    return new OperationResult<byte[]>(StatusCode.COLUMN_NOT_FOUND);
  }

  @Override
  public OperationResult<byte[]> getDirty(byte[] row, byte[] column) throws OperationException {
    try {
      Get get = new Get(row);
      get.addColumn(this.family, column);
      get.setTimeRange(0, Long.MAX_VALUE);
      get.setMaxVersions();
      Result result = this.readTable.get(get);
      if (!result.isEmpty()) {
        return new OperationResult<byte[]>(result.value());
      } else {
        return new OperationResult<byte[]>(StatusCode.COLUMN_NOT_FOUND);
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
    // as fall-back return "not found".
    return new OperationResult<byte[]>(StatusCode.COLUMN_NOT_FOUND);
  }

  protected long getMaxStamp(ReadPointer readPointer) {
    return readPointer.getMaximum() == Long.MAX_VALUE ? readPointer.getMaximum() : readPointer.getMaximum() + 1;
  }

  @Override
  public OperationResult<ImmutablePair<byte[], Long>> getWithVersion(byte[] row, byte[] column, ReadPointer readPointer)
    throws OperationException {
    Set<Long> deleted = new HashSet<Long>();
    try {
      Get get = new Get(row);
      get.addColumn(this.family, column);
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();
      Result result = this.readTable.get(get);
      for (KeyValue kv : result.raw()) {
        long version = kv.getTimestamp();
        if (!readPointer.isVisible(version) || deleted.contains(version)) {
          continue;
        }
        byte[] value = kv.getValue();
        byte typePrefix = value[0];
        switch (typePrefix) {
          case DATA:
            byte[] trueValue = removeTypePrefix(value);
            return new OperationResult<ImmutablePair<byte[], Long>>(new ImmutablePair<byte[], Long>(trueValue,
                                                                                                    version));
          case DELETE_VERSION:
            deleted.add(version);
            break;
          case DELETE_ALL:
            return null;
          //return new OperationResult<byte[]>(null); ???
        }
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
    // as fall-back return "not found".
    return new OperationResult<ImmutablePair<byte[], Long>>(StatusCode.COLUMN_NOT_FOUND);
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> get(byte[] row, byte[] startColumn, byte[] stopColumn, int limit,
                                                  ReadPointer readPointer)
    throws OperationException {
    // limit, startColumn and stopColumn refer to number of columns, not values!
    boolean done = false;

    try {
      Get get = new Get(row);
      get.addFamily(this.family);
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();

      // negative limit means unlimited, map that to int.max
      if (limit <= 0) {
        limit = Integer.MAX_VALUE;
      }
      Map<byte[], byte[]> map = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
      int currentLimit = limit;
      byte[] currentStartColumn = startColumn;
      byte[] currentLatestColumn = null;
      int currentResultSize;
      while (!done) {
        // push down the column range and the limit into the get as a filter
        List<Filter> filters = Lists.newArrayList();
        if (currentStartColumn != null || stopColumn != null) {
          filters.add(new ColumnRangeFilter(currentStartColumn, true, stopColumn, false));
        }
        if (currentLimit != Integer.MAX_VALUE) {
          filters.add(new ColumnPaginationFilter(currentLimit, 0));
        }
        if (filters.size() > 1) {
          get.setFilter(new FilterList(filters));
        } else if (filters.size() == 1) {
          get.setFilter(filters.get(0));
        }
        Result result = this.readTable.get(get);
        currentResultSize = result.size();
        byte[] previousColumn = null;

        Set<Long> currentDeleted = new HashSet<Long>();
        for (KeyValue kv : result.raw()) {
          byte[] column = kv.getQualifier();
          currentLatestColumn = column;
          // filter out versions that are invisible under current ReadPointer
          long version = kv.getTimestamp();
          if (!readPointer.isVisible(version)) {
            continue;
          }
          // make sure that we skip repeated occurrences of the same column -
          // they would be older revisions that overwrite the most recent one
          // in the result map!
          if (currentDeleted.contains(version)) {
            currentDeleted.remove(version);
            continue;
          }
          if (Bytes.equals(previousColumn, column)) {
            continue;
          }
          byte[] value = kv.getValue();
          byte typePrefix = value[0];
          if (typePrefix == DATA) {
            map.put(column, removeTypePrefix(value));
            currentDeleted.clear();
          }
          if (typePrefix == DELETE_ALL) {
            currentDeleted.clear();
          }
          if (typePrefix == DELETE_VERSION) {
            currentDeleted.add(version);
          }

          // add to the result
          // and remember this column to be able to filter out older revisions
          // of the same column (which would follow next in the hbase result)
          previousColumn = column;
        }
        if (limit != Integer.MAX_VALUE && map.size() < limit && currentResultSize == currentLimit) {
          done = false;
          currentLimit = limit - map.size();
          currentStartColumn = Bytes.incrementBytes(currentLatestColumn, 1);
        } else {
          done = true;
        }

      }
      if (map.isEmpty()) {
        return new OperationResult<Map<byte[], byte[]>>(StatusCode.COLUMN_NOT_FOUND);
      } else {
        return new OperationResult<Map<byte[], byte[]>>(map);
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
    // as fall-back return "not found".
    return new OperationResult<Map<byte[], byte[]>>(StatusCode.COLUMN_NOT_FOUND);
  }

  @Override
  public List<byte[]> getKeys(int limit, int offset, ReadPointer readPointer) throws OperationException {
    //invisible (due to readPointer) and deleted cells (due to DELETED_VERSION or DELETED_ALL)
    //do not count towards limit and offset
    //negative limit means unlimited, map that to int.max
    if (limit <= 0) {
      limit = Integer.MAX_VALUE;
    }
    List<byte[]> keys = new ArrayList<byte[]>(limit > 1024 ? 1024 : limit);
    int returnedRow = 0;
    int skippedRow = 0;
    Set<Long> deletedCellsWithinRow = Sets.newHashSet();
    boolean fastForwardToNextColumn = false;  // due to DeleteAll tombstone
    boolean fastForwardToNextRow = false;
    byte[] previousRow = null;
    byte[] previousColumn = null;
    try {
      Scan scan = new Scan();
      scan.setTimeRange(0, getMaxStamp(readPointer));
      scan.setMaxVersions();
      ResultScanner scanner = this.readTable.getScanner(scan);
      Result result;
      while ((result = scanner.next()) != null) {
        for (KeyValue kv : result.raw()) {
          byte[] row = kv.getRow();
          if (Bytes.equals(previousRow, row) && fastForwardToNextRow) {
            continue;
          }
          fastForwardToNextRow = false;
          byte[] column = kv.getQualifier();
          if (Bytes.equals(previousColumn, column) && fastForwardToNextColumn) {
            continue;
          }
          fastForwardToNextColumn = false;
          long version = kv.getTimestamp();
          if (!readPointer.isVisible(version)) {
            continue;
          }
          if (deletedCellsWithinRow.contains(version)) {
            deletedCellsWithinRow.remove(version);
            continue;
          }
          byte[] value = kv.getValue();
          byte typePrefix = value[0];
          if (typePrefix == DATA) {
            //found row with at least one cell with DATA
            if (skippedRow < offset) {
              skippedRow++;
            } else if (returnedRow < limit) {
              returnedRow++;
              keys.add(kv.getRow());
            }
            if (returnedRow == limit) {
              return keys;
            }
            fastForwardToNextRow = true;
            fastForwardToNextColumn = false;
          }
          if (typePrefix == DELETE_ALL) {
            fastForwardToNextColumn = true;
            deletedCellsWithinRow.clear();
          }
          if (typePrefix == DELETE_VERSION) {
            deletedCellsWithinRow.add(version);
          }
          previousColumn = column;
          previousRow = row;
        }
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
    return keys;
  }

  @Override
  public long increment(byte[] row, byte[] column, long amount, ReadPointer readPointer, long writeVersion)
    throws OperationException {
    HTable writeTable = null;
    try {
      KeyValue kv = getLatestVisible(row, column, readPointer);
      long value = amount;
      if (kv != null) {
        try {
          value += Bytes.toLong(removeTypePrefix(kv.getValue()));
        } catch (IllegalArgumentException e) {
          throw new OperationException(StatusCode.ILLEGAL_INCREMENT, e.getMessage(), e);
        }
      }
      writeTable = getWriteTable();
      writeTable.put(
        new Put(row).add(this.family, column, writeVersion, prependWithTypePrefix(DATA, Bytes.toBytes(value))));
      return value;
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
      return -1L;
    } finally {
      if (writeTable != null) {
        returnWriteTable(writeTable);
      }
    }
  }

  @Override
  public Map<byte[], Long> increment(byte[] row, byte[][] columns, long[] amounts, ReadPointer readPointer,
                                     long writeVersion)
    throws OperationException {
    Map<byte[], Long> ret = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
    HTable writeTable = null;
    List<Put> puts = new ArrayList<Put>(columns.length);
    try {
      KeyValue[] kvs = getLatestVisible(row, columns, readPointer);
      for (int i = 0; i<columns.length; i++) {
        KeyValue kv = kvs[i];
        long l = amounts[i];
        if (kv!=null) {
          l += Bytes.toLong(removeTypePrefix(kv.getValue()));
        }
        Put put = new Put(row);
        put.add(this.family, columns[i], writeVersion, prependWithTypePrefix(DATA, Bytes.toBytes(l)));
        puts.add(put);
        ret.put(columns[i], l);
      }
      writeTable = getWriteTable();
      writeTable.put(puts);
      return ret;
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
      ret = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
      return ret;
    } finally {
      if (writeTable != null) {
        returnWriteTable(writeTable);
      }
    }
  }

  @Override
  public long incrementAtomicDirtily(byte[] row, byte[] column, long amount) throws OperationException {
    // Note: HBase increment does not take a write version, to keep compareAndSwapDirty compatible with increments
    // compareAndSwapDirty too does not use an explicit write version.
    HTable writeTable = null;
    try {
      writeTable = getWriteTable();
      return writeTable.incrementColumnValue(row, this.family, column, amount);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
      return -1L;
    } finally {
      if (writeTable != null) {
        returnWriteTable(writeTable);
      }
    }
  }

  private KeyValue getLatestVisible(final byte[] row, final byte[] qualifier, ReadPointer readPointer)
    throws OperationException {
    Set<Long> deleted = Sets.newHashSet();
    try {
      Get get = new Get(row);
      get.addColumn(this.family, qualifier);
      // read rows that were written up until the start of the current transaction (=getMaxStamp(readPointer))
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();
      Result result = this.readTable.get(get);
      for (KeyValue kv : result.raw()) {
        long version = kv.getTimestamp();
        if (!readPointer.isVisible(version) || deleted.contains(version)) {
          continue;
        }
        byte[] value = kv.getValue();
        byte typePrefix = value[0];
        switch (typePrefix) {
          case DATA:
            return kv;
          case DELETE_VERSION:
            deleted.add(version);
            break;
          case DELETE_ALL:
            return null;
        }
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
    return null;
  }

  private boolean equalValues(KeyValue keyValue, byte[] value) {
    return ((value == null && keyValue == null) ||
      (value == null && keyValue.getValue() == null) ||
      (value != null && keyValue != null && Bytes.equals(removeTypePrefix(keyValue.getValue()), value)));
  }

  @Override
  public void compareAndSwap(byte[] row, byte[] column, byte[] expectedValue, byte[] newValue,
                             ReadPointer readPointer, long writeVersion)
    throws OperationException {
    // Note: Since this CAS operation is not atomic - a Get followed by a Put - there is a risk of a conflict when
    // two transacations try to update the same column. This is ok, since opex will make sure that at commit time
    // one of the transactions will fail.
    KeyValue latestVisibleKV = null;
    HTable writeTable = null;
    try {
      Get get = new Get(row);
      get.addColumn(this.family, column);
      // read rows that were written up until the start of the current transaction (=getMaxStamp(readPointer))
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();
      Result result = this.readTable.get(get);
      KeyValue[] rawResults = result.raw();
      if (rawResults != null && rawResults.length != 0) {
        Set<Long> deleted = Sets.newHashSet();
        for (KeyValue kv : result.raw()) {
          long version = kv.getTimestamp();
          if (!readPointer.isVisible(version) || deleted.contains(version)) {
            continue;
          }
          byte[] value = kv.getValue();
          byte typePrefix = value[0];
          if (typePrefix == DATA) {
            latestVisibleKV = kv;
            break;
          } else if (typePrefix == DELETE_VERSION) {
            deleted.add(version);
          } else if (typePrefix == DELETE_ALL) {
            latestVisibleKV = null;
            break;
          }
        }
      }
      if (equalValues(latestVisibleKV, expectedValue)) {
        byte[] newPrependedValue;
        if (newValue == null) {
          newPrependedValue = DELETE_ALL_VALUE;
        } else {
          newPrependedValue = prependWithTypePrefix(DATA, newValue);
        }
        writeTable = getWriteTable();
        writeTable.put(new Put(row).add(this.family, column, writeVersion, newPrependedValue));
      } else {
        throw new OperationException(StatusCode.WRITE_CONFLICT, "CompareAndSwap expected value mismatch");
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    } finally {
      if (writeTable != null) {
        returnWriteTable(writeTable);
      }
    }
  }

  @Override
  public boolean compareAndSwapDirty(byte[] row, byte[] column, byte[] expectedValue, byte[] newValue)
    throws OperationException {
    // Note: HBase increment does not take a write version, to keep compareAndSwapDirty compatible with increments
    // compareAndSwapDirty too does not use an explicit write version (true for vanilla HBase, right now an explicit
    // version is used since the patched HBase only has checkAndPut with version).
    HTable writeTable = null;
    try {
      writeTable = getWriteTable();
      Put put = new Put(row);
      put.add(this.family, column, newValue);
      // TODO: need to use checkAndPut without version (vanilla HBase)
      return writeTable.checkAndPut(row, this.family, column, expectedValue, put);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    } finally {
      if (writeTable != null) {
        returnWriteTable(writeTable);
      }
    }
    throw new OperationException(StatusCode.INTERNAL_ERROR, "This point should not be reached");
  }

  @Override
  public void clear() throws OperationException {
    try {
      HBaseAdmin hba = new HBaseAdmin(conf);
      HTableDescriptor htd = hba.getTableDescriptor(tableName);
      hba.disableTable(tableName);
      hba.deleteTable(tableName);
      hba.createTable(htd);
    } catch (IOException ioe) {
      this.exceptionHandler.handle(ioe);
    }
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow, ReadPointer readPointer) {
    ResultScanner resultScanner = null;
    try {
      Scan scan =  new Scan();
      if (startRow != null) {
        scan.setStartRow(startRow);
      }
      if (stopRow != null) {
        scan.setStopRow(stopRow);
      }
      scan.setTimeRange(0, getMaxStamp(readPointer));
      scan.setMaxVersions();
      resultScanner = this.readTable.getScanner(scan);
    } catch (IOException e) {
      Throwables.propagate(e);
    }
    return new HBaseScanner(resultScanner, readPointer);
  }

  /**
   * Interface to handle exceptions from HBase.
   */
  public static interface IOExceptionHandler {
    public void handle(IOException e) throws OperationException;
  }

  /**
   * An exception handler that converts every Hbase exception to an OperationException.
   */
  @SuppressWarnings("unused")
  public static class ToOperationExceptionHandler implements IOExceptionHandler {
    @Override
    public void handle(IOException e) throws OperationException {
      String msg = "HBase IO exception: " + e.getMessage();
      Log.error(msg, e);
      throw new OperationException(StatusCode.HBASE_ERROR, msg, e);
    }
  }

  @SuppressWarnings("unused")
  private void dumpColumn(byte[] row, byte[] column) throws OperationException {
    try {
      Get get = new Get(row);
      get.addColumn(this.family, column);
      get.setTimeRange(0, Long.MAX_VALUE);
      get.setMaxVersions();
      Result result = this.readTable.get(get);
      for (KeyValue kv : result.raw()) {
        long version = kv.getTimestamp();
        byte[] value = kv.getValue();
        if (value == null || value.length == 0) {
          Log.error("value == null || value.length");
        }
        Log.debug("{}.{}:{}.{} -> {}", Bytes.toString(row), Bytes.toString(family), column, version, value);
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
  }

  @SuppressWarnings("unused")
  private void dumpTable() throws OperationException {
    try {
      Scan scan = new Scan();
      scan.setTimeRange(0, Long.MAX_VALUE);
      scan.setMaxVersions();
      ResultScanner scanner = this.readTable.getScanner(scan);
      Result result;
      while ((result = scanner.next()) != null) {
        for (KeyValue kv : result.raw()) {
          byte[] row = kv.getRow();
          byte[] column = kv.getQualifier();
          long version = kv.getTimestamp();
          byte[] value = kv.getValue();
          if (value == null || value.length == 0) {
            Log.error("value == null || value.length");
          }
          Log.debug("{}.{}:{}.{} -> {}", Bytes.toString(row), Bytes.toString(family), column, version, value);
        }
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
  }

  /**
   * Implements Scanner on top of HBase resultSetScanner.
   */
  public class HBaseScanner implements Scanner {

    private final ResultScanner scanner;
    private final ReadPointer readPointer;

    public HBaseScanner(ResultScanner scanner, ReadPointer readPointer){
      this.scanner = scanner;
      this.readPointer = readPointer;
    }

    @Override
    public ImmutablePair<byte[], Map<byte[], byte[]>> next() {
      if (scanner == null) {
        return null;
      }

      try {
        Map<byte[], byte[]> colValue = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        byte [] rowKey = null;
        byte[] last = null;

        //Loop until one row is read completely or until end is reached.
        while (colValue.isEmpty()) {
          Result result = scanner.next();
          if (result == null) {
            break;
          } else {
            Set<Long> deleted = Sets.newHashSet();
            for (KeyValue kv : result.raw()){

              long version = kv.getTimestamp();
              if (readPointer != null && !readPointer.isVisible(version)) {
                continue;
              }

              if (deleted.contains(version)){
                continue;
              }

              byte[] value = kv.getValue();
              byte[] column = kv.getQualifier();

              if (Bytes.equals(column, last)){
                continue;
              }

              byte typePrefix = value[0];
              switch (typePrefix) {
                case DATA:
                  colValue.put(column, removeTypePrefix(kv.getValue()));
                  last = column;
                  if (rowKey == null) {
                    rowKey = kv.getRow();
                  }
                  deleted.clear(); //Read one version. So clear deleted hash to get non deleted versions of next cols.
                  break;
                case DELETE_VERSION:
                  deleted.add(version);
                  break;
                case DELETE_ALL:
                  last = column; //Mark not to read any versions of this column
                  break;
                default:
                  break;
              }
            }
          }
        }

        if (colValue.isEmpty()) {
          return null;
        } else {
          return new ImmutablePair<byte[], Map<byte[], byte[]>>(rowKey, colValue);
        }
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void close() {
      scanner.close();
    }
  }

  /**
   * Gets latest visible KeyValue for given row, qualifier, and readPointer. Accepts array of qualifiers and returns
   * array of corresponded KeyValues. Length of the returned array equals to array of qualifiers, with the KeyValue at
   * index i corresponded to qualifier at index i in passed parameter. If no visible KeyValue, the element of the
   * returned array is null.
   * <p>
   *   Using this method for array of qualifiers is supposedly more efficient than using
   *   {@link #getLatestVisible(byte[], byte[], com.continuuity.data.operation.executor.ReadPointer)} for every
   *   qualifier separately.
   * </p>
   * @param row row value
   * @param qualifiers array of qualifiers
   * @param readPointer readPointer value
   * @return an array of values according to description above
   * @throws OperationException
   */
  private KeyValue[] getLatestVisible(final byte[] row, final byte[][] qualifiers, ReadPointer readPointer)
    throws OperationException {
    KeyValue[] keyVals = new KeyValue[qualifiers.length];
    Set<Long> deleted = Sets.newHashSet();
    try {
      // Using one Get operation to fetch data for all qualifiers at once
      Get get = new Get(row);
      for (byte[] qualifier : qualifiers) {
        get.addColumn(this.family, qualifier);
      }
      // read rows that were written up until the start of the current transaction (=getMaxStamp(readPointer))
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();
      Result result = this.readTable.get(get);
      if (result.isEmpty()) {
        return keyVals;
      }
      for (int i = 0; i < qualifiers.length; i++) {
        for (KeyValue kv : result.getColumn(this.family, qualifiers[i])) {
          long version = kv.getTimestamp();
          if (!readPointer.isVisible(version) || deleted.contains(version)) {
            continue;
          }
          byte[] value = kv.getValue();
          byte typePrefix = value[0];
          boolean found = false;
          switch (typePrefix) {
            case DATA:
              keyVals[i] = kv;
              found = true;
              break;
            case DELETE_VERSION:
              deleted.add(version);
              break;
            case DELETE_ALL:
              found = true;
              // in this case return val = null
              break;
          }
          if (found) {
            break;
          }
        }
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
    return keyVals;
  }
}
