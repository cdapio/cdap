package com.continuuity.data2.dataset2.lib.table.inmemory;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.table.Row;
import com.continuuity.api.dataset.table.Scanner;
import com.continuuity.data2.dataset.lib.table.FuzzyRowFilter;
import com.continuuity.data2.dataset2.lib.table.Result;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * An in-memory implememtation of a scanner.
 */
public class InMemoryScanner implements Scanner {

  private final Iterator<Map.Entry<byte[], NavigableMap<byte[], byte[]>>> rows;
  private final Set<byte[]> columnsToInclude;
  private final FuzzyRowFilter filter;

  public InMemoryScanner(Iterator<Map.Entry<byte[], NavigableMap<byte[], byte[]>>> rows) {
    this(rows, null, null);
  }

  public InMemoryScanner(Iterator<Map.Entry<byte[], NavigableMap<byte[], byte[]>>> rows, FuzzyRowFilter filter,
                         byte[][] columnsToInclude) {
    this.rows = rows;
    this.filter = filter;
    if (columnsToInclude != null) {
      this.columnsToInclude = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
      Collections.addAll(this.columnsToInclude, columnsToInclude);
    } else {
      this.columnsToInclude = null;
    }
  }

  @Override
  public Row next() {
    Map<byte[], byte[]> columns = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    Map.Entry<byte[], NavigableMap<byte[], byte[]>> rowEntry = null;

    while (columns.isEmpty() && this.rows.hasNext()) {
      rowEntry = this.rows.next();
      if (filter != null) {
        FuzzyRowFilter.ReturnCode code = filter.filterRow(rowEntry.getKey());
        if (FuzzyRowFilter.ReturnCode.DONE.equals(code)) {
          break; // no more rows can match
        } else if (!FuzzyRowFilter.ReturnCode.INCLUDE.equals(code)) {
          continue; // this row does not match filter, move to next row
        }
      }
      //Try to read all columns for this row
      for (Map.Entry<byte[], byte[]> colEntry : rowEntry.getValue().entrySet()) {
        if (columnsToInclude == null || columnsToInclude.contains(colEntry.getKey())) {
          columns.put(colEntry.getKey(), colEntry.getValue());
        }
      }
    }
    if (columns.size() > 0) {
      assert rowEntry != null;
      return new Result(rowEntry.getKey(), columns);
    } else {
      return null;
    }
  }

  @Override
  public void close() { /* no op */ }

}
