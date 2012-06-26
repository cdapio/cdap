package com.continuuity.data.engine.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.ReadPointer;
import com.continuuity.data.table.Scanner;

public class HBaseOVCTable implements OrderedVersionedColumnarTable {

  private final HTable table;

  private final byte[] family;

  private final IOExceptionHandler exceptionHandler;

  public HBaseOVCTable(HTable table, final byte[] family,
      IOExceptionHandler exceptionHandler) {
    this.table = table;
    this.family = family;
    this.exceptionHandler = exceptionHandler;
  }

  @Override
  public void put(byte[] row, byte[] column, long version, byte[] value) {
    try {
      this.table.put(new Put(row).add(this.family, column, version, value));
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
  }

  @Override
  public void put(byte[] row, byte[][] columns, long version, byte[][] values) {
    assert (columns.length == values.length);
    try {
      Put put = new Put(row);
      for (int i = 0; i < columns.length; i++) {
        put.add(this.family, columns[i], version, values[i]);
      }
      this.table.put(put);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
  }

  @Override
  public void delete(byte[] row, byte[] column, long version) {
    try {
      Delete delete = new Delete(row);
      delete.deleteColumn(this.family, column, version);
      this.table.delete(delete);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
  }

  @Override
  public void delete(byte[] row, byte[][] columns, long version) {
    try {
      Delete delete = new Delete(row);
      for (byte [] column : columns)
        delete.deleteColumn(this.family, column, version);
      this.table.delete(delete);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
  }

  @Override
  public void deleteAll(byte[] row, byte[] column, long version) {
    try {
      Delete delete = new Delete(row);
      delete.deleteColumns(this.family, column, version);
      this.table.delete(delete);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
  }

  @Override
  public void deleteAll(byte[] row, byte[][] columns, long version) {
    try {
      Delete delete = new Delete(row);
      for (byte [] column : columns)
        delete.deleteColumns(this.family, column, version);
      this.table.delete(delete);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
  }

  @Override
  public void undeleteAll(byte[] row, byte[] column, long version) {
    throw new RuntimeException("undelete operation not supported by hbase");
  }

  @Override
  public void undeleteAll(byte[] row, byte[][] columns, long version) {
    throw new RuntimeException("undelete operation not supported by hbase");
  }

  @Override
  public Map<byte[], byte[]> get(byte[] row, ReadPointer readPointer) {
    try {
      Get get = new Get(row);
      get.addFamily(this.family);
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();
      Result result = this.table.get(get);
      Map<byte[], byte[]> map = new TreeMap<byte[], byte[]>(
          Bytes.BYTES_COMPARATOR);
      byte[] last = null;
      for (KeyValue kv : result.raw()) {
        long version = kv.getTimestamp();
        if (!readPointer.isVisible(version)) continue;
        byte [] column = kv.getQualifier();
        if (Bytes.equals(last, column)) continue;
        map.put(column, kv.getValue());
        last = column;
      }
      return map;
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
      return null;
    }
  }

  @Override
  public byte[] get(byte[] row, byte[] column, ReadPointer readPointer) {
    try {
      Get get = new Get(row);
      get.addColumn(this.family, column);
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();
      Result result = this.table.get(get);
      for (KeyValue kv : result.raw()) {
        long version = kv.getTimestamp();
        if (!readPointer.isVisible(version)) continue;
        return kv.getValue();
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
    return null;
  }

  private long getMaxStamp(ReadPointer readPointer) {
    return readPointer.getMaximum() == Long.MAX_VALUE ?
        readPointer.getMaximum() : readPointer.getMaximum() + 1;
  }

  @Override
  public ImmutablePair<byte[], Long> getWithVersion(byte[] row, byte[] column,
      ReadPointer readPointer) {
    try {
      Get get = new Get(row);
      get.addColumn(this.family, column);
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();
      Result result = this.table.get(get);
      for (KeyValue kv : result.raw()) {
        long version = kv.getTimestamp();
        if (!readPointer.isVisible(version)) continue;
        return new ImmutablePair<byte[],Long>(kv.getValue(), kv.getTimestamp());
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
    return null;
  }

  @Override
  public Map<byte[], byte[]> get(byte[] row, byte[] startColumn,
      byte[] stopColumn, ReadPointer readPointer) {
    try {
      Get get = new Get(row);
      get.addFamily(this.family);
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();
      Result result = this.table.get(get);
      Map<byte[], byte[]> map = new TreeMap<byte[], byte[]>(
          Bytes.BYTES_COMPARATOR);
      byte[] last = null;
      for (KeyValue kv : result.raw()) {
        long version = kv.getTimestamp();
        if (!readPointer.isVisible(version)) continue;
        byte [] column = kv.getQualifier();
        if (Bytes.equals(last, column)) continue;
        map.put(column, kv.getValue());
        last = column;
      }
      return map;
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
      return null;
    }
  }

  @Override
  public Map<byte[], byte[]> get(byte[] row, byte[][] columns,
      ReadPointer readPointer) {
    try {
      Get get = new Get(row);
      for (byte [] column : columns) get.addColumn(this.family, column);
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();
      Result result = this.table.get(get);
      Map<byte[], byte[]> map = new TreeMap<byte[], byte[]>(
          Bytes.BYTES_COMPARATOR);
      byte[] last = null;
      for (KeyValue kv : result.raw()) {
        long version = kv.getTimestamp();
        if (!readPointer.isVisible(version)) continue;
        byte [] column = kv.getQualifier();
        if (Bytes.equals(last, column)) continue;
        map.put(column, kv.getValue());
        last = column;
      }
      return map;
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
      return null;
    }
  }

  @Override
  public List<byte[]> getKeys(int limit, int offset, ReadPointer readPointer) {
    List<byte[]> keys = new ArrayList<byte[]>(limit > 1024 ? 1024 : limit);
    int returned = 0;
    int skipped = 0;
    try {
      Scan scan = new Scan();
      scan.setTimeRange(0, getMaxStamp(readPointer));
      scan.setMaxVersions();
      ResultScanner scanner = this.table.getScanner(scan);
      Result result = null;
      while ((result = scanner.next()) != null) {
        for (KeyValue kv : result.raw()) {
          if (!readPointer.isVisible(kv.getTimestamp())) continue;
          if (skipped < offset) {
            skipped++;
          } else if (returned < limit) {
            returned++;
            keys.add(kv.getRow());
          }
          if (returned == limit) return keys;
          break;
        }
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
    return keys;
  }

  @Override
  public long increment(byte[] row, byte[] column, long amount,
      ReadPointer readPointer, long writeVersion) {
    try {
      // TODO: This currently does not support passing a read pointer or a write
      //       pointer!
      Increment increment = new Increment(row);
      increment.addColumn(this.family, column, amount);
      increment.setTimeRange(0, getMaxStamp(readPointer));
      Result result = this.table.increment(increment);
      if (result.isEmpty()) return 0L;
      return Bytes.toLong(result.value());
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
      return -1L;
    }
  }

  @Override
  public Map<byte[], Long> increment(byte[] row, byte[][] columns,
      long[] amounts, ReadPointer readPointer, long writeVersion) {
    Map<byte[],Long> ret = new TreeMap<byte[],Long>(Bytes.BYTES_COMPARATOR);
    try {
      // TODO: This currently does not support passing a read pointer or a write
      //       pointer!
      Increment increment = new Increment(row);
      increment.setTimeRange(0, getMaxStamp(readPointer));
      for (int i=0; i<columns.length; i++)
        increment.addColumn(this.family, columns[i], amounts[i]);
      Result result = this.table.increment(increment);
      for (KeyValue kv : result.raw())
        ret.put(kv.getRow(), Bytes.toLong(kv.getValue()));
      return ret;
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
      return ret;
    }
  }

  @Override
  public boolean compareAndSwap(byte[] row, byte[] column,
      byte[] expectedValue, byte[] newValue, ReadPointer readPointer,
      long writeVersion) {
    try {
      // TODO: This currently does not support passing a read pointer!
      Put put = new Put(row);
      put.add(this.family, column, writeVersion, newValue);
      return this.table.checkAndPut(row, this.family, column, expectedValue, put);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
      return false;
    }
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow,
      ReadPointer readPointer) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow,
      byte[][] columns, ReadPointer readPointer) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Scanner scan(ReadPointer readPointer) {
    // TODO Auto-generated method stub
    return null;
  }

  public class HBaseScanner implements Scanner {

    public HBaseScanner(Scanner scanner) {

    }
    @Override
    public ImmutablePair<byte[], Map<byte[], byte[]>> next() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub

    }

  }
  public static interface IOExceptionHandler {
    public void handle(IOException e);
  }

  public static class IOExceptionToRuntimeExceptionHandler implements
  IOExceptionHandler {
    @Override
    public void handle(IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void format() {
    // TODO Auto-generated method stub
    
  }


}
