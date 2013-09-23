package com.continuuity.data2.dataset.lib.table;

import com.continuuity.api.data.OperationResult;

import java.util.Map;

/**
 * Implements some of the methods in a generic way (not necessarily in most efficient way)
 */
public abstract class AbstractOrderedColumnarTable implements OrderedColumnarTable {
  @Override
  public byte[] get(byte[] row, byte[] column) throws Exception {
    OperationResult<Map<byte[], byte[]>> result = get(row, new byte[][]{column});
    return result.isEmpty() ? null : result.getValue().get(column);
  }

  @Override
  public void put(byte [] row, byte [] column, byte[] value) throws Exception {
    put(row, new byte[][] {column}, new byte[][] {value});
  }

  @Override
  public void delete(byte[] row, byte[] column) throws Exception {
    delete(row, new byte[][] {column});
  }
}
