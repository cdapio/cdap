package com.continuuity.data.table.converter;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.table.ColumnarTable;
import com.continuuity.data.table.TimeSeriesTable;

public class TimeSeriesOnColumnarTable implements TimeSeriesTable {

  private final ColumnarTable table;
  
  public TimeSeriesOnColumnarTable(ColumnarTable table) {
    this.table = table;
  }
  
  @Override
  public void addPoint(byte[] key, long time, byte[] value) {
    table.put(key, Bytes.toBytes(reverse(time)), value);
  }

  @Override
  public byte[] getPoint(byte[] key, long time) {
    return table.get(key, Bytes.toBytes(reverse(time)));
  }

  @Override
  public Map<Long, byte[]> getPoints(byte[] key, long startTime, long endTime) {
    Map<byte[],byte[]> columns = table.get(key, Bytes.toBytes(reverse(endTime)),
        Bytes.toBytes(reverse(startTime)));
    Map<Long,byte[]> ret = new TreeMap<Long,byte[]>();
    for (Map.Entry<byte[], byte[]> entry : columns.entrySet()) {
      ret.put(Bytes.toLong(entry.getKey()), entry.getValue());
    }
    return ret;
  }

  private long reverse(long stamp) {
    return Long.MAX_VALUE - stamp;
  }
}
