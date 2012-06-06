package com.continuuity.data.engine.hbase;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
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
      delete.deleteFamily(this.family, version);
      this.table.delete(delete);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
  }

  @Override
  public Map<byte[], byte[]> get(byte[] row, ReadPointer readPointer) {
    try {
      Get get = new Get(row);
      get.addFamily(this.family);
      get.setTimeRange(0, readPointer.getMaximum() + 1);
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
      get.setTimeRange(0, readPointer.getMaximum() + 1);
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

  @Override
  public ImmutablePair<byte[], Long> getWithVersion(byte[] row, byte[] column,
      ReadPointer readPointer) {
    try {
      Get get = new Get(row);
      get.addColumn(this.family, column);
      get.setTimeRange(0, readPointer.getMaximum() + 1);
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
      get.setTimeRange(0, readPointer.getMaximum() + 1);
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
      get.setTimeRange(0, readPointer.getMaximum() + 1);
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
  public long increment(byte[] row, byte[] column, long amount,
      ReadPointer readPointer, long writeVersion) {
    try {
      // TODO: This currently does not support passing a read pointer or a write
      //       pointer!
      Increment increment = new Increment(row);
      increment.addColumn(family, column, amount);
      increment.setTimeRange(0, readPointer.getMaximum());
      Result result = this.table.increment(increment);
      if (result.isEmpty()) return 0L;
      return Bytes.toLong(result.value());
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
      return -1L;
    }
  }

  @Override
  public boolean compareAndSwap(byte[] row, byte[] column,
      byte[] expectedValue, byte[] newValue, ReadPointer readPointer,
      long writeVersion) {
    try {
      // TODO: This currently does not support passing a read pointer!
      Put put = new Put(row);
      put.add(family, column, writeVersion, newValue);
      return this.table.checkAndPut(row, family, column, expectedValue, put);
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

//  @Override
//  public void deleteAll(byte[] row, byte[] column, long version) {
//    // TODO Auto-generated method stub
//
//  }
}
