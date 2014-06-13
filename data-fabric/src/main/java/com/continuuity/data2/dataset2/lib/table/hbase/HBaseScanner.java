package com.continuuity.data2.dataset2.lib.table.hbase;

import com.continuuity.api.dataset.table.Row;
import com.continuuity.api.dataset.table.Scanner;
import com.continuuity.data2.transaction.Transaction;
import com.google.common.base.Throwables;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.Map;

/**
 * Implements Scanner on top of HBase resultSetScanner.
 */
class HBaseScanner implements Scanner {

  private final ResultScanner scanner;
  private final Transaction tx;

  public HBaseScanner(ResultScanner scanner, Transaction tx) {
    this.scanner = scanner;
    this.tx = tx;
  }

  @Override
  public Row next() {
    if (scanner == null) {
      return null;
    }

    try {

      //Loop until one row is read completely or until end is reached.
      while (true) {
        Result result = scanner.next();
        if (result == null || result.isEmpty()) {
          break;
        }

        Map<byte[], byte[]> rowMap = HBaseOrderedTable.getRowMap(result, tx);
        if (rowMap.size() > 0) {
          return new com.continuuity.data2.dataset2.lib.table.Result(result.getRow(), rowMap);
        }
      }

      // exhausted
      return null;

    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void close() {
    scanner.close();
  }
}
