package com.continuuity.data.engine.hbase;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.table.Scanner;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * This implements the OVCTable for an "improved" HBase that has extensions to handle queues and transactions.
 */
public class HBaseNativeOVCTable extends HBaseOVCTable {

  public HBaseNativeOVCTable(CConfiguration cConf, Configuration hConf, final byte[] tableName, final byte[] family,
                             IOExceptionHandler exceptionHandler)
    throws OperationException {
    super(cConf, hConf, tableName, family, exceptionHandler);
  }

  private synchronized HTable getWriteTable() throws IOException {
    HTable writeTable = this.writeTables.pollFirst();
    return writeTable == null ? new HTable(this.conf, this.tableName) : writeTable;
  }

  private synchronized void returnWriteTable(HTable table) {
    this.writeTables.add(table);
  }

  @Override
  public void put(byte[] row, byte[][] columns, long version, byte[][] values) throws OperationException {
    assert (columns.length == values.length);
    HTable writeTable = null;
    try {
      writeTable = getWriteTable();
      Put put = new Put(row);
      for (int i = 0; i < columns.length; i++) {
        put.add(this.family, columns[i], version, values[i]);
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
        put.add(this.family, columns[i], version, values[i]);
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
  public void put(byte[][] rows, byte[][][] columnsPerRow, long version, byte[][][] valuesPerRow)
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
          put.add(this.family, columns[j], version, values[j]);
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
  public void put(byte[] row, byte[] column, long version, byte[] value) throws OperationException {
    put(row, new byte[][]{column}, version, new byte[][]{value});
  }

  @Override
  public void delete(byte[] row, byte[][] columns, long version) throws OperationException {
    HTable writeTable = null;
    try {
      writeTable = getWriteTable();
      Delete delete = new Delete(row);
      for (byte[] column : columns) {
        delete.deleteColumn(this.family, column, version);
      }
      writeTable.delete(delete);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    } finally {
      if (writeTable != null) {
        returnWriteTable(writeTable);
      }
    }
  }

  @Override
  public void delete(byte[] row, byte[] column, long version) throws OperationException {
    delete(row, new byte[][]{column}, version);
  }

  @Override
  public void deleteAll(byte[] row, byte[][] columns, long version) throws OperationException {
    HTable writeTable = null;
    try {
      writeTable = getWriteTable();
      Delete delete = new Delete(row);
      for (byte[] column : columns) {
        delete.deleteColumns(this.family, column, version);
      }
      writeTable.delete(delete);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    } finally {
      if (writeTable != null) {
        returnWriteTable(writeTable);
      }
    }
  }

  @Override
  public void deleteAll(byte[] row, byte[] column, long version) throws OperationException {
    deleteAll(row, new byte[][]{column}, version);
  }

  @Override
  public void undeleteAll(byte[] row, byte[][] columns, long version) throws OperationException {
    HTable writeTable = null;
    try {
      writeTable = getWriteTable();
      Delete delete = new Delete(row);
      for (byte[] column : columns) {
        delete.undeleteColumns(this.family, column, version);
      }
      writeTable.delete(delete);
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
    try {
      Get get = new Get(row);
      get.addFamily(this.family);
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();
      Result result = this.readTable.get(get);
      Map<byte[], byte[]> map = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
      byte[] last = null;
      for (KeyValue kv : result.raw()) {
        long version = kv.getTimestamp();
        if (!readPointer.isVisible(version)) {
          continue;
        }
        byte[] column = kv.getQualifier();
        if (Bytes.equals(last, column)) {
          continue;
        }
        map.put(column, kv.getValue());
        last = column;
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
  public OperationResult<byte[]> get(byte[] row, byte[] column, ReadPointer readPointer) throws OperationException {
    try {
      Get get = new Get(row);
      get.addColumn(this.family, column);
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();
      Result result = this.readTable.get(get);
      for (KeyValue kv : result.raw()) {
        long version = kv.getTimestamp();
        if (!readPointer.isVisible(version)) {
          continue;
        }
        byte[] value = kv.getValue();
        if (value == null || value.length == 0) {
          return new OperationResult<byte[]>(StatusCode.COLUMN_NOT_FOUND);
        } else {
          return new OperationResult<byte[]>(value);
        }
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
    // as fall-back return "not found".
    return new OperationResult<byte[]>(StatusCode.COLUMN_NOT_FOUND);
  }

  @Override
  public OperationResult<ImmutablePair<byte[], Long>> getWithVersion(byte[] row, byte[] column, ReadPointer readPointer)
    throws OperationException {
    try {
      Get get = new Get(row);
      get.addColumn(this.family, column);
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();
      Result result = this.readTable.get(get);
      for (KeyValue kv : result.raw()) {
        long version = kv.getTimestamp();
        if (!readPointer.isVisible(version)) {
          continue;
        }
        return new OperationResult<ImmutablePair<byte[], Long>>(new ImmutablePair<byte[], Long>(kv.getValue(),
                                                                                                kv.getTimestamp()));
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
    try {
      // prepare a get for hbase
      Get get = new Get(row);
      get.addFamily(this.family);
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();

      // negative limit means unlimited, map that to int.max
      if (limit <= 0) {
        limit = Integer.MAX_VALUE;
      }

      // push down the column range and the limit into the get as a filter
      List<Filter> filters = Lists.newArrayList();
      if (startColumn != null || stopColumn != null) {
        filters.add(new ColumnRangeFilter(startColumn, true, stopColumn, false));
      }
      if (limit != Integer.MAX_VALUE) {
        filters.add(new ColumnPaginationFilter(limit, 0));
      }
      if (filters.size() > 1) {
        get.setFilter(new FilterList(filters));
      } else if (filters.size() == 1) {
        get.setFilter(filters.get(0));
      }

      Result result = this.readTable.get(get);
      Map<byte[], byte[]> map = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
      byte[] last = null;
      for (KeyValue kv : result.raw()) {
        // filter out versions that are invisible under current ReadPointer
        long version = kv.getTimestamp();
        if (!readPointer.isVisible(version)) {
          continue;
        }
        // make sure that we skip repeated occurrences of the same column -
        // they would be older revisions that overwrite the most recent one
        // in the result map!
        byte[] column = kv.getQualifier();
        if (Bytes.equals(last, column)) {
          continue;
        }
        // add to the result
        map.put(kv.getQualifier(), kv.getValue());
        // and remember this column to be able to filter out older revisions
        // of the same column (which would follow next in the hbase result)
        last = column;
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
    Map<byte[], byte[]> map = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    byte[] last = null;
    for (KeyValue kv : result.raw()) {
      long version = kv.getTimestamp();
      if (!readPointer.isVisible(version)) {
        continue;
      }
      byte[] column = kv.getQualifier();
      if (Bytes.equals(last, column)) {
        continue;
      }
      map.put(column, kv.getValue());
      last = column;
    }
    return map;
  }

  @Override
  public OperationResult<Map<byte[], Map<byte[], byte[]>>> getAllColumns(byte[][] rows, byte[][] columns,
                                                                         ReadPointer readPointer)
    throws OperationException {
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
      Result[] results = this.readTable.get(gets);
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
    return new OperationResult<Map<byte[], Map<byte[], byte[]>>>(StatusCode.KEY_NOT_FOUND);
  }

  @Override
  public List<byte[]> getKeys(int limit, int offset, ReadPointer readPointer) throws OperationException {
    List<byte[]> keys = new ArrayList<byte[]>(limit > 1024 ? 1024 : limit);
    int returned = 0;
    int skipped = 0;
    try {
      Scan scan = new Scan();
      scan.setTimeRange(0, getMaxStamp(readPointer));
      scan.setMaxVersions();
      ResultScanner scanner = this.readTable.getScanner(scan);
      Result result;
      while ((result = scanner.next()) != null) {
        for (KeyValue kv : result.raw()) {
          if (!readPointer.isVisible(kv.getTimestamp())) {
            continue;
          }
          if (skipped < offset) {
            skipped++;
          } else if (returned < limit) {
            returned++;
            keys.add(kv.getRow());
          }
          if (returned == limit) {
            return keys;
          }
          break;
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
    try {
      Increment increment = new Increment(row);
      increment.addColumn(this.family, column, amount);
      increment.setTimeRange(0, getMaxStamp(readPointer));
      increment.setWriteVersion(writeVersion);
      Result result = this.readTable.increment(increment);
      if (result.isEmpty()) {
        return 0L;
      }
      return Bytes.toLong(result.value());
    } catch (IOException e) {
      // figure out whether this is an illegal increment
      // currently there is not other way to extract that from the HBase exception than string match
      if (e.getMessage() != null && e.getMessage().contains("isn't 64 bits wide")) {
        throw new OperationException(StatusCode.ILLEGAL_INCREMENT, e.getMessage(), e);
      }
      this.exceptionHandler.handle(e);
      return -1L;
    }
  }

  @Override
  public Map<byte[], Long> increment(byte[] row, byte[][] columns, long[] amounts, ReadPointer readPointer,
                                     long writeVersion)
    throws OperationException {
    Map<byte[], Long> ret = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
    try {
      Increment increment = new Increment(row);
      increment.setTimeRange(0, getMaxStamp(readPointer));
      increment.setWriteVersion(writeVersion);
      for (int i = 0; i < columns.length; i++) {
        increment.addColumn(this.family, columns[i], amounts[i]);
      }
      Result result = this.readTable.increment(increment);
      for (KeyValue kv : result.raw()) {
        ret.put(kv.getQualifier(), Bytes.toLong(kv.getValue()));
      }
      return ret;
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
      return ret;
    }
  }

  @Override
  public void compareAndSwap(byte[] row, byte[] column, byte[] expectedValue, byte[] newValue,
                             ReadPointer readPointer, long writeVersion)
    throws OperationException {
    try {
      if (newValue == null) {
        Delete delete = new Delete(row);
        delete.deleteColumns(this.family, column, writeVersion);
        if (!this.readTable.checkAndDelete(row, this.family, column, expectedValue, readPointer.getMaximum(), delete)) {
          throw new OperationException(StatusCode.WRITE_CONFLICT, "CompareAndSwap expected value mismatch");
        }
      } else {
        Put put = new Put(row);
        put.add(this.family, column, writeVersion, newValue);
        if (!this.readTable.checkAndPut(row, this.family, column, expectedValue, readPointer.getMaximum(), put)) {
          throw new OperationException(StatusCode.WRITE_CONFLICT, "CompareAndSwap expected value mismatch");
        }
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
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
      throw Throwables.propagate(e);
    }
    return new HBaseNativeScanner(resultScanner, readPointer);
  }

  /**
   * Implements Scanner on top of HBase ResultSetScanner for native Hbase.
   */
  public class HBaseNativeScanner implements Scanner {

    private final ResultScanner scanner;
    private final ReadPointer readPointer;

    public HBaseNativeScanner(ResultScanner scanner, ReadPointer readPointer) {
      this.scanner = scanner;
      this.readPointer = readPointer;
    }

    @Override
    public ImmutablePair<byte[], Map<byte[], byte[]>> next() {
      if (scanner == null) {
        return null;
      }
      try {
        Result result = scanner.next();
        if (result == null) {
          return null;
        }
        Map<byte[], byte[]> colValue = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        byte [] rowKey = null;
        byte[] last = null;

        for (KeyValue kv : result.raw()) {

          if (readPointer != null && !readPointer.isVisible(kv.getTimestamp())) {
            continue;
          }

          rowKey = kv.getRow();
          byte[] column = kv.getQualifier();
          if (Bytes.equals(column, last)) {
            continue;
          }

          byte [] value = kv.getValue();
          last = column;
          colValue.put(column, value);
        }

        if (rowKey == null) {
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

}
