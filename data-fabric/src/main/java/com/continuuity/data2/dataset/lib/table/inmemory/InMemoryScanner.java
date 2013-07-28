package com.continuuity.data2.dataset.lib.table.inmemory;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.table.Scanner;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * An in-memory implememtation of a scanner.
 */
public class InMemoryScanner implements Scanner {

  private final Iterator<Map.Entry<byte[], NavigableMap<byte[], byte[]>>> rows;

  public InMemoryScanner(Iterator<Map.Entry<byte[], NavigableMap<byte[], byte[]>>> rows) {
    this.rows = rows;
  }

  @Override
  public ImmutablePair<byte[], Map<byte[], byte[]>> next() {
    Map<byte[], byte[]> columns = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    Map.Entry<byte[], NavigableMap<byte[], byte[]>> rowEntry = null;

    while (columns.size() == 0 && this.rows.hasNext()){
      rowEntry = this.rows.next();
      //Try to read all columns for this row
      for (Map.Entry<byte[], byte[]> colEntry : rowEntry.getValue().entrySet()) {
        columns.put(colEntry.getKey(), colEntry.getValue());
      }
    }
    if (columns.size() > 0) {
      return new ImmutablePair<byte[], Map<byte[], byte[]>>(rowEntry.getKey(), columns);
    } else {
      return null;
    }
  }

  @Override
  public void close() { /* no op */ }

}
