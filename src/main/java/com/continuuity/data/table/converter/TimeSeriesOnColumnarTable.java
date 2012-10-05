package com.continuuity.data.table.converter;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.utils.Bytes;
import com.continuuity.data.table.ColumnarTable;
import com.continuuity.data.table.TimeSeriesTable;

import java.util.Map;
import java.util.TreeMap;

public class TimeSeriesOnColumnarTable implements TimeSeriesTable {

  private final ColumnarTable table;
  
  public TimeSeriesOnColumnarTable(ColumnarTable table) {
    this.table = table;
  }
  
  @Override
  public void addPoint(byte[] key, long time, byte[] value)
      throws OperationException {
    table.put(key, Bytes.toBytes(reverse(time)), value);
  }

  @Override
  public OperationResult<byte[]> getPoint(byte[] key, long time)
      throws OperationException {
    return table.get(key, Bytes.toBytes(reverse(time)));
  }

  @Override
  public Map<Long, byte[]> getPoints(byte[] key, long startTime, long endTime)
      throws OperationException {
    OperationResult<Map<byte[], byte[]>> columns =
        table.get(key, Bytes.toBytes(reverse(endTime)),
            Bytes.toBytes(reverse(startTime)));
    Map<Long,byte[]> ret = new TreeMap<Long,byte[]>();
    for (Map.Entry<byte[], byte[]> entry : columns.getValue().entrySet()) {
      ret.put(Bytes.toLong(entry.getKey()), entry.getValue());
    }
    return ret;
  }

  private long reverse(long stamp) {
    return Long.MAX_VALUE - stamp;
  }
}
