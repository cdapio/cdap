package com.continuuity.data2.dataset.lib.table.inmemory;

import com.continuuity.api.common.Bytes;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.dataset.lib.table.BackedByVersionedStoreOcTableClient;
import com.continuuity.data2.dataset.lib.table.ConflictDetection;
import com.continuuity.data2.transaction.Transaction;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import javax.annotation.Nullable;

/**
 *
 */
public class InMemoryOcTableClient extends BackedByVersionedStoreOcTableClient {
  private static final long NO_TX_VERSION = 0L;

  private Transaction tx;

  public InMemoryOcTableClient(String name) {
    this(name, ConflictDetection.ROW);
  }

  public InMemoryOcTableClient(String name, ConflictDetection level) {
    super(name, level);
  }

  @Override
  public void startTx(Transaction tx) {
    super.startTx(tx);
    this.tx = tx;
  }

  @Override
  protected void persist(NavigableMap<byte[], NavigableMap<byte[], Update>> buff) {
    // split up the increments and puts
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> puts = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    NavigableMap<byte[], NavigableMap<byte[], Long>> increments = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], NavigableMap<byte[], Update>> rowEntry : buff.entrySet()) {
      for (Map.Entry<byte[], Update> colEntry : rowEntry.getValue().entrySet()) {
        Update val = colEntry.getValue();
        if (val instanceof IncrementValue) {
          NavigableMap<byte[], Long> incrCols = increments.get(rowEntry.getKey());
          if (incrCols == null) {
            incrCols = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
            increments.put(rowEntry.getKey(), incrCols);
          }
          incrCols.put(colEntry.getKey(), ((IncrementValue) val).getValue());
        } else if (val instanceof PutValue) {
          NavigableMap<byte[], byte[]> putCols = puts.get(rowEntry.getKey());
          if (putCols == null) {
            putCols = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
            puts.put(rowEntry.getKey(), putCols);
          }
          putCols.put(colEntry.getKey(), ((PutValue) val).getValue());
        }
      }
    }
    InMemoryOcTableService.merge(getTableName(), puts, tx.getWritePointer());
    for (Map.Entry<byte[], NavigableMap<byte[], Long>> incrRow : increments.entrySet()) {
      InMemoryOcTableService.increment(getTableName(), incrRow.getKey(), incrRow.getValue());
    }
  }

  @Override
  protected void undo(NavigableMap<byte[], NavigableMap<byte[], Update>> persisted) {
    // NOTE: we could just use merge and pass the changes with all values = null, but separate method is more efficient
    InMemoryOcTableService.undo(getTableName(), persisted, tx.getWritePointer());
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
  protected NavigableMap<byte[], byte[]> getPersisted(byte[] row, @Nullable byte[][] columns) throws Exception {
    return getInternal(row, columns);
  }

  @Override
  protected Scanner scanPersisted(byte[] startRow, byte[] stopRow) {
    // todo: a lot of inefficient copying from one map to another
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowRange =
      InMemoryOcTableService.getRowRange(getTableName(), startRow, stopRow, tx == null ? null : tx.getReadPointer());
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> visibleRowRange = getLatestNotExcludedRows(rowRange, tx);
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> rows = unwrapDeletesForRows(visibleRowRange);

    return new InMemoryScanner(rows.entrySet().iterator());
  }

  private NavigableMap<byte[], byte[]> getInternal(byte[] row, @Nullable byte[][] columns) throws IOException {
    // no tx logic needed
    if (tx == null) {
      NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap =
        InMemoryOcTableService.get(getTableName(), row, NO_TX_VERSION);

      return unwrapDeletes(filterByColumns(getLatest(rowMap), columns));
    }

    NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap =
      InMemoryOcTableService.get(getTableName(), row, tx.getReadPointer());

    if (rowMap == null) {
      return EMPTY_ROW_MAP;
    }

    // if exclusion list is empty, do simple "read last" value call todo: explain
    if (!tx.hasExcludes()) {
      return unwrapDeletes(filterByColumns(getLatest(rowMap), columns));
    }

    NavigableMap<byte[], byte[]> result = filterByColumns(getLatestNotExcluded(rowMap, tx), columns);
    return unwrapDeletes(result);
  }

  private NavigableMap<byte[], byte[]> filterByColumns(NavigableMap<byte[], byte[]> rowMap,
                                                       @Nullable byte[][] columns) {
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
      // latest go first
      result.put(column.getKey(), column.getValue().firstEntry().getValue());
    }
    return result;
  }
}
