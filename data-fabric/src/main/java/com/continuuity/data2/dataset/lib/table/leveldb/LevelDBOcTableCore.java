package com.continuuity.data2.dataset.lib.table.leveldb;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.engine.leveldb.KeyValue;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.transaction.Transaction;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Provides common operations for levelDB tables and queues.
 */
public class LevelDBOcTableCore {

  private static final Logger LOG = LoggerFactory.getLogger(LevelDBOcTableCore.class);

  // this represents deleted values
  protected static final byte[] DELETE_MARKER = { };

  // we use the empty column family for all data
  private static final byte[] DATA_COLFAM = { };

  // we will never write this, but use it as an upper bound for scans
  private static final byte[] NEXT_COLFAM = { 0x00 };

  // used for obtaining the next row/column for upper bound
  private static final byte[] ONE_ZERO = { 0x00 };

  private static byte[] upperBound(byte[] column) {
    return Bytes.add(column, ONE_ZERO);
  }

  // empty immutable row's column->value map constant
  static final NavigableMap<byte[], byte[]> EMPTY_ROW_MAP =
    Maps.unmodifiableNavigableMap(Maps.<byte[], byte[], byte[]>newTreeMap(Bytes.BYTES_COMPARATOR));

  private final WriteOptions writeOptions;
  private final DB db;

  public LevelDBOcTableCore(String tableName, LevelDBOcTableService service) throws IOException {
    this.db = service.getTable(tableName);
    this.writeOptions = service.getWriteOptions();
  }

  public void persist(NavigableMap<byte[], NavigableMap<byte[], byte[]>> changes, long version) throws IOException {
    WriteBatch batch = db.createWriteBatch();
    for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> row : changes.entrySet()) {
      for (Map.Entry<byte[], byte[]> column : row.getValue().entrySet()) {
        byte[] key = createPutKey(row.getKey(), column.getKey(), version);
        batch.put(key, column.getValue() == null ? DELETE_MARKER : column.getValue());
      }
    }
    db.write(batch, writeOptions);
  }

  public void undo(NavigableMap<byte[], NavigableMap<byte[], byte[]>> persisted, long version) {
    if (persisted.isEmpty()) {
      return;
    }
    WriteBatch batch = db.createWriteBatch();
    for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> row : persisted.entrySet()) {
      for (Map.Entry<byte[], byte[]> column : row.getValue().entrySet()) {
        byte[] key = createPutKey(row.getKey(), column.getKey(), version);
        batch.delete(key);
      }
    }
    db.write(batch, writeOptions);
  }

  public Scanner scan(byte[] startRow, byte[] stopRow, Transaction tx) throws IOException {
    DBIterator iterator = db.iterator();
    try {
      if (startRow != null) {
        iterator.seek(createStartKey(startRow));
      } else {
        iterator.seekToFirst();
      }
    } catch (RuntimeException e) {
      try {
        iterator.close();
      } catch (IOException ioe) {
        LOG.warn("Error closing LevelDB iterator", ioe);
        // but what else can we do? nothing...
      }
      throw e;
    }
    byte[] endKey = stopRow == null ? null : createEndKey(stopRow);
    return new LevelDBScanner(iterator, endKey, tx);
  }

  public NavigableMap<byte[], byte[]> getRow(byte[] row, byte[][] columns, byte[] startCol, byte[] stopCol,
                                             int limit, Transaction tx) throws IOException {
    if (columns != null) {
      if (columns.length == 0) {
        return EMPTY_ROW_MAP;
      }
      columns = Arrays.copyOf(columns, columns.length);
      Arrays.sort(columns, Bytes.BYTES_COMPARATOR);
    }

    byte[] startKey = createStartKey(row, columns == null ? startCol : columns[0]);
    byte[] endKey = createEndKey(row, columns == null ? stopCol : upperBound(columns[columns.length - 1]));
    DBIterator iterator = db.iterator();
    try {
      iterator.seek(startKey);
      return getRow(iterator, endKey, tx, false, columns, limit).getSecond();
    } finally {
      iterator.close();
    }
  }

  /**
   * Read one row of the table. This is used both by getRow() and by Scanner.next().
   * @param iterator An iterator over the database. This is passed in such that the caller can reuse the same
   *                 iterator if scanning multiple rows.
   * @param endKey An upper bound for the (leveldb) keys to read. This method never reads past that key.
   * @param tx The transaction to use for visibility.
   * @param multiRow If true indicates that the row may end before the endKey. In that case,
   *                 this method will stop reading as soon as it sees more than one row key. The iterator will not be
   *                 advanced past the beginning of the next row (so that next time, we still see the entire next row).
   * @param columns If non-null, only columns contained in this will be returned.
   * @param limit If non-negative, at most this many columns will be returned. If multiRow is true, this is ignored.
   * @return a pair consisting of the row key of the next non-empty row and the column map for that row. If multiRow
   *         is false, null is returned for row key because the caller already knows it.
   */
  private static ImmutablePair<byte[], NavigableMap<byte[], byte[]>>
  getRow(DBIterator iterator, byte[] endKey, Transaction tx, boolean multiRow, byte[][] columns, int limit)
    throws IOException {

    byte[] rowBeingRead = null;
    byte[] previousCol = null;
    NavigableMap<byte[], byte[]> map = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);

    while (iterator.hasNext()) {
      Map.Entry<byte[], byte[]> entry = iterator.peekNext();

      // if we have reached past the endKey, nothing was found, return null
      if (endKey != null && KeyValue.KEY_COMPARATOR.compare(entry.getKey(), endKey) >= 0) {
        break;
      }

      // if this is part of a multi-row scan and we reach the next row, stop without advancing iterator
      KeyValue kv = KeyValue.fromKey(entry.getKey());
      if (multiRow) {
        byte[] rowKey = kv.getRow();
        if (rowBeingRead != null) {
          if (Bytes.compareTo(rowKey, rowBeingRead) > 0) {
            break;
          }
        }
      }

      // it is safe to consume this entry, advance the iterator
      iterator.next();

      // Determine if this KV is visible
      if (tx != null && !tx.isVisible(kv.getTimestamp())) {
        continue;
      }

      // have we seen this column before?
      byte[] column = kv.getQualifier();
      if (previousCol != null && Bytes.equals(previousCol, column)) {
        continue;
      }
      // remember that this is the last column we have seen
      previousCol = column;

      // is it a column we want?
      if (columns == null || Arrays.binarySearch(columns, column, Bytes.BYTES_COMPARATOR) >= 0) {
        byte[] value = entry.getValue();
        // only add to map if it is not a delete
        if (!Bytes.equals(value, DELETE_MARKER)) {
          map.put(column, value);
          // first time we add a column. must remember the row key to know when to stop
          if (multiRow && rowBeingRead == null) {
            rowBeingRead = kv.getRow();
          }
          if (limit > 0 && map.size() >= limit) {
            break;
          }
        }
      }
    }
    // note this will return null for the row being read if multiRow is false (because the caller knows the row)
    return new ImmutablePair<byte[], NavigableMap<byte[], byte[]>>(rowBeingRead, map);
  }

  /**
   * Delete a list of rows from the table entirely, disregarding transactions.
   * @param toDelete the sorted list of row keys to delete.
   */
  public void deleteRows(List<byte[]> toDelete) throws IOException {
    if (toDelete.isEmpty()) {
      return;
    }
    // find first row to delete and first entry in the DB to examine
    Iterator<byte[]> rows = toDelete.iterator();
    byte[] currentRow = rows.next();
    byte[] startKey = createStartKey(currentRow);
    DBIterator iterator = db.iterator();
    WriteBatch batch = db.createWriteBatch();
    try {
      iterator.seek(startKey);
      if (!iterator.hasNext()) {
        return; // nothing in the db to delete
      }
      Map.Entry<byte[], byte[]> entry = iterator.next();

      // iterate over the database and the rows to delete, collecting (raw) keys to delete
      while (entry != null && currentRow != null) {
        KeyValue kv = KeyValue.fromKey(entry.getKey());
        int comp = Bytes.compareTo(kv.getRow(), currentRow);
        if (comp == 0) {
          // same row -> delete
          batch.delete(entry.getKey());
          entry = iterator.hasNext() ? iterator.next() : null;
        } else if (comp > 0) {
          // read past current row -> move to next row
          currentRow = rows.hasNext() ? rows.next() : null;
        } else if (comp < 0) {
          // iterator must seek to current row
          iterator.seek(createStartKey(currentRow));
          entry = iterator.hasNext() ? iterator.next() : null;
        }
      }
    } finally {
      iterator.close();
    }
    // delete all the entries that were found
    db.write(batch, writeOptions);
  }

  /**
   * A scanner for a range of rows.
   */
  private static class LevelDBScanner implements Scanner {

    private final Transaction tx;
    private byte[] endKey;
    private final DBIterator iterator;

    public LevelDBScanner(DBIterator iterator, byte[] endKey, Transaction tx) {
      this.tx = tx;
      this.endKey = endKey;
      this.iterator = iterator;
    }

    @Override
    public ImmutablePair<byte[], Map<byte[], byte[]>> next() {
      try {
        ImmutablePair<byte[], NavigableMap<byte[], byte[]>> result = getRow(iterator, endKey, tx, true, null, -1);
        return result.getSecond().isEmpty() ? null :
          ImmutablePair.of(result.getFirst(), (Map<byte[], byte[]>) result.getSecond());
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void close() {
      try {
        iterator.close();
      } catch (Exception e) {
        LOG.warn("Error closing LevelDB iterator", e);
        // but what else can we do? nothing.
      }
    }
  }

  // ------- helpers to create the keys for writes and scans ----------

  private static byte[] createPutKey(byte[] rowKey, byte[] columnKey, long version) {
    return new KeyValue(rowKey, DATA_COLFAM, columnKey, version, KeyValue.Type.Put).getKey();
  }

  private static byte[] createStartKey(byte[] row) { // the first possible key of a row
    return new KeyValue(row, DATA_COLFAM, null, KeyValue.LATEST_TIMESTAMP, KeyValue.Type.Maximum).getKey();
  }
  private static byte[] createEndKey(byte[] row) {
    return createStartKey(row); // the first key of the stop is the first to be excluded
  }

  private static byte[] createStartKey(byte[] row, byte[] column) {
    return new KeyValue(row, DATA_COLFAM, column, KeyValue.LATEST_TIMESTAMP, KeyValue.Type.Maximum).getKey();
  }

  private static byte[] createEndKey(byte[] row, byte[] column) {
    if (column != null) {
      // we have a stop column and can use that as an upper bound
      return new KeyValue(row, DATA_COLFAM, column, KeyValue.LATEST_TIMESTAMP, KeyValue.Type.Maximum).getKey();
    } else {
      // no stop column - use next column family as upper bound
      return new KeyValue(row, NEXT_COLFAM, null, KeyValue.LATEST_TIMESTAMP, KeyValue.Type.Maximum).getKey();
    }
  }
}
