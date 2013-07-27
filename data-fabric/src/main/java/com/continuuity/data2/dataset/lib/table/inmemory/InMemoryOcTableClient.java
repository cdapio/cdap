package com.continuuity.data2.dataset.lib.table.inmemory;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.dataset.lib.table.BackedByVersionedStoreOcTableClient;
import com.continuuity.data2.dataset.lib.table.hbase.HBaseOcTableClient;
import com.continuuity.data2.transaction.Transaction;
import com.google.common.collect.Maps;
import com.sun.tools.javac.resources.version;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 *
 */
public class InMemoryOcTableClient extends BackedByVersionedStoreOcTableClient {
  private static final long NO_TX_VERSION = 0L;

  private Transaction tx;

  public InMemoryOcTableClient(String name) {
    super(name);
  }

  @Override
  public void startTx(Transaction tx) {
    super.startTx(tx);
    this.tx = tx;
  }

  @Override
  protected void persist(NavigableMap<byte[], NavigableMap<byte[], byte[]>> buff) {
    InMemoryOcTableService.merge(getName(), buff, tx.getWritePointer());
  }

  @Override
  protected NavigableMap<byte[], byte[]> getPersisted(byte[] row) throws IOException {
    return getInternal(row, null);
  }

  @Override
  protected byte[] getPersisted(byte[] row, byte[] column) throws Exception {
    NavigableMap<byte[], byte[]> internal = getInternal(row, new byte[][]{column});
    return internal.get(column);
  }

  @Override
  protected NavigableMap<byte[], byte[]> getPersisted(byte[] row, byte[] startColumn, byte[] stopColumn, int limit)
    throws Exception {

    NavigableMap<byte[], byte[]> rowMap = getInternal(row, null);
    if (rowMap == null) {
      return EMPTY_ROW_MAP;
    }
    return getRange(rowMap, startColumn, stopColumn, limit);
  }

  @Override
  protected NavigableMap<byte[], byte[]> getPersisted(byte[] row, byte[][] columns) throws Exception {
    return getInternal(row, columns);
  }

  @Override
  protected Scanner scanPersisted(byte[] startRow, byte[] stopRow) {
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowRange =
      InMemoryOcTableService.getRowRange(getName(), startRow, stopRow, tx.getReadPointer());

    NavigableMap<byte[], NavigableMap<byte[], byte[]>> visibleRowRange =
      getLatestNotExcludedRows(rowRange, tx.getExcludedList());

    return new MemoryScanner(visibleRowRange.entrySet().iterator());
  }

  private NavigableMap<byte[], byte[]> getInternal(byte[] row, byte[][] columns) throws IOException {
    // no tx logic needed
    if (tx == null) {
      NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap =
        InMemoryOcTableService.get(getName(), row, NO_TX_VERSION);

      return unwrapDeletes(filterByColumns(getLatest(rowMap), columns));
    }

    NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap =
      InMemoryOcTableService.get(getName(), row, tx.getReadPointer());

    if (rowMap == null) {
      return EMPTY_ROW_MAP;
    }

    // if exclusion list is empty, do simple "read last" value call todo: explain
    if (tx.getExcludedList().length == 0) {
      return unwrapDeletes(filterByColumns(getLatest(rowMap), columns));
    }

    NavigableMap<byte[], byte[]> result = filterByColumns(getLatestNotExcluded(rowMap, tx.getExcludedList()), columns);
    return unwrapDeletes(result);
  }

  private NavigableMap<byte[], byte[]> filterByColumns(NavigableMap<byte[], byte[]> rowMap, byte[][] columns) {
    if (columns == null) {
      return rowMap;
    }
    NavigableMap<byte[], byte[]> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (byte[] column : columns) {
      byte[] val = rowMap.get(column);
      if (val != null) {
        result.put(column, val);
      }
    }
    return result;

  }

  private NavigableMap<byte[], byte[]> getLatest(NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap) {
    if (rowMap == null) {
      return EMPTY_ROW_MAP;
    }

    NavigableMap<byte[], byte[]> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], NavigableMap<Long, byte[]>> column : rowMap.entrySet()) {
      result.put(column.getKey(), column.getValue().lastEntry().getValue());
    }
    return result;
  }


  /**
   * An in-memory implememtation of a scanner.
   */
  public class MemoryScanner implements Scanner {

    private final Iterator<Map.Entry<byte[], NavigableMap<byte[], byte[]>>> rows;

    private final Set<byte[]> columnSet;

    public MemoryScanner(Iterator<Map.Entry<byte[], NavigableMap<byte[], byte[]>>> rows) {
      this(rows, null);
    }

    public MemoryScanner(Iterator<Map.Entry<byte[], NavigableMap<byte[], byte[]>>> rows,
                         byte[][] columns) {
      this.rows = rows;
      this.columnSet = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
      if (columns != null) {
        Collections.addAll(this.columnSet, columns);
      }
    }

    @Override
    public ImmutablePair<byte[], Map<byte[], byte[]>> next() {
      Map<byte[], byte[]> columns = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
      Map.Entry<byte[], NavigableMap<byte[], byte[]>> rowEntry = null;
      boolean gotNext = false;

      while (!gotNext){
        if (!this.rows.hasNext()){
          break;
        }
        rowEntry = this.rows.next();
        //Try to read all columns for this row
        for (Map.Entry<byte[], byte[]> colEntry : rowEntry.getValue().entrySet()) {
          if (!this.columnSet.isEmpty() && !this.columnSet.contains(colEntry.getKey())) {
            continue;
          }
          columns.put(colEntry.getKey(), colEntry.getValue());
        }
        if (columns.size() > 0) {
          //there is at least one valid col for row. Exit the loop. If not try next row
          gotNext =  true;
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
}
