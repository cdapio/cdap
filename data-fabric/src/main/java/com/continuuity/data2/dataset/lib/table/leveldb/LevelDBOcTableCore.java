package com.continuuity.data2.dataset.lib.table.leveldb;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.dataset.lib.table.FuzzyRowFilter;
import com.continuuity.data2.transaction.Transaction;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import javax.annotation.Nullable;

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
  // Using ImmutableSortedMap instead of Maps.unmodifiableNavigableMap to avoid conflicts with
  // Hadoop, which uses an older version of guava without that method.
  static final NavigableMap<byte[], byte[]> EMPTY_ROW_MAP =
    ImmutableSortedMap.<byte[], byte[]>orderedBy(Bytes.BYTES_COMPARATOR).build();

  private final String tableName;
  private final LevelDBOcTableService service;

  public LevelDBOcTableCore(String tableName, LevelDBOcTableService service) throws IOException {
    this.tableName = tableName;
    this.service = service;
  }

  private DB getDB() throws IOException {
    return service.getTable(tableName);
  }

  private WriteOptions getWriteOptions() {
    return service.getWriteOptions();
  }


  public synchronized boolean swap(byte[] row, byte[] column, byte[] oldValue, byte[] newValue) throws IOException {
    byte[] existing = getRow(row, new byte[][] { column }, null, null, -1, null).get(column);
    // verify
    if (oldValue == null && existing != null) {
      return false;
    }
    if (oldValue != null && (existing == null || !Bytes.equals(oldValue, existing))) {
      return false;
    }
    // write
    if (newValue == null) {
      // to-do
      deleteColumn(row, column);
    } else {
      persist(Collections.singletonMap(row, Collections.singletonMap(column, newValue)), System.currentTimeMillis());
    }
    return true;
  }

  public synchronized Map<byte[], Long> increment(byte[] row, Map<byte[], Long> increments) throws Exception {
    NavigableMap<byte[], byte[]> existing =
      getRow(row, increments.keySet().toArray(new byte[increments.size()][]), null, null, -1, null);
    Map<byte[], Long> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    Map<byte[], byte[]> replacing = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], Long> increment : increments.entrySet()) {
      long existingValue = 0L;
      byte[] existingBytes = existing.get(increment.getKey());
      if (existingBytes != null) {
        if (existingBytes.length != Bytes.SIZEOF_LONG) {
          throw new NumberFormatException("Attempted to increment a value that is not convertible to long," +
                                            " row: " + Bytes.toStringBinary(row) +
                                            " column: " + Bytes.toStringBinary(increment.getKey()));
        }
        existingValue = Bytes.toLong(existingBytes);
      }
      long newValue = existingValue + increment.getValue();
      result.put(increment.getKey(), newValue);
      replacing.put(increment.getKey(), Bytes.toBytes(newValue));
    }
    persist(ImmutableMap.of(row, replacing), System.currentTimeMillis());
    return result;
  }

  public void persist(Map<byte[], ? extends Map<byte[], byte[]>> changes, long version) throws IOException {
    DB db = getDB();
    // todo support writing null when no transaction
    WriteBatch batch = db.createWriteBatch();
    for (Map.Entry<byte[], ? extends Map<byte[], byte[]>> row : changes.entrySet()) {
      for (Map.Entry<byte[], byte[]> column : row.getValue().entrySet()) {
        byte[] key = createPutKey(row.getKey(), column.getKey(), version);
        batch.put(key, column.getValue() == null ? DELETE_MARKER : column.getValue());
      }
    }
    db.write(batch, service.getWriteOptions());
  }

  public void put(byte[] row, byte[] column, byte[] value, long version) throws IOException {
    getDB().put(createPutKey(row, column, version), value);
  }

  public void undo(Map<byte[], ? extends Map<byte[], byte[]>> persisted, long version) throws IOException {
    if (persisted.isEmpty()) {
      return;
    }
    DB db = getDB();
    WriteBatch batch = db.createWriteBatch();
    for (Map.Entry<byte[], ? extends Map<byte[], byte[]>> row : persisted.entrySet()) {
      for (Map.Entry<byte[], byte[]> column : row.getValue().entrySet()) {
        byte[] key = createPutKey(row.getKey(), column.getKey(), version);
        batch.delete(key);
      }
    }
    db.write(batch, service.getWriteOptions());
  }

  public Scanner scan(byte[] startRow, byte[] stopRow,
                      @Nullable FuzzyRowFilter filter, @Nullable byte[][] columns, @Nullable Transaction tx)
    throws IOException {
    DBIterator iterator = getDB().iterator();
    seekToStart(iterator, startRow);
    byte[] endKey = stopRow == null ? null : createEndKey(stopRow);
    return new LevelDBScanner(iterator, endKey, filter, columns, tx);
  }

  public NavigableMap<byte[], byte[]> getRow(byte[] row, @Nullable byte[][] columns,
                                             byte[] startCol, byte[] stopCol,
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
    DBIterator iterator = getDB().iterator();
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
    byte[] previousRow = null;
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

      // have we seen this row & column before?
      byte[] row = kv.getRow();
      byte[] column = kv.getQualifier();
      boolean seenThisColumn = previousRow != null && Bytes.equals(previousRow, row) &&
                               previousCol != null && Bytes.equals(previousCol, column);
      if (seenThisColumn) {
        continue;
      }
      // remember that this is the last column we have seen
      previousRow = row;
      previousCol = column;

      // is it a column we want?
      if (columns == null || Arrays.binarySearch(columns, column, Bytes.BYTES_COMPARATOR) >= 0) {
        byte[] value = entry.getValue();
        // only add to map if it is not a delete
        if (tx == null || !Bytes.equals(value, DELETE_MARKER)) {
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

  public void deleteRows(byte[] prefix) throws IOException {
    Preconditions.checkNotNull(prefix, "prefix must not be null");
    DB db = getDB();
    WriteBatch batch = db.createWriteBatch();
    DBIterator iterator = db.iterator();
    try {
      iterator.seek(createStartKey(prefix));
      while (iterator.hasNext()) {
        Map.Entry<byte[], byte[]> entry = iterator.next();
        if (!Bytes.startsWith(KeyValue.fromKey(entry.getKey()).getRow(), prefix)) {
          // iterator is past prefix
          break;
        }
        batch.delete(entry.getKey());
      }
      db.write(batch);
    } finally {
      iterator.close();
    }
  }

  /**
   * Delete a list of rows from the table entirely, disregarding transactions.
   * @param toDelete the row keys to delete
   */
  public void deleteRows(Collection<byte[]> toDelete) throws IOException {
    if (toDelete.isEmpty()) {
      return;
    }
    // find first row to delete and first entry in the DB to examine
    Iterator<byte[]> rows = toDelete.iterator();
    byte[] currentRow = rows.next();
    byte[] startKey = createStartKey(currentRow);
    DB db = getDB();
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
    db.write(batch, getWriteOptions());
  }

  public void deleteRange(byte[] startRow, byte[] stopRow, @Nullable FuzzyRowFilter filter, @Nullable byte[][] columns)
    throws IOException {
    DB db = getDB();
    DBIterator iterator = db.iterator();
    seekToStart(iterator, startRow);
    byte[] endKey = stopRow == null ? null : createEndKey(stopRow);
    Scanner scanner = new LevelDBScanner(iterator, endKey, filter, columns, null);

    DBIterator deleteIterator = db.iterator();
    seekToStart(deleteIterator, startRow);
    final int deletesPerRound = 1024; // todo make configurable
    try {
      ImmutablePair<byte[], Map<byte[], byte[]>> rowValues;
      WriteBatch batch = db.createWriteBatch();
      int deletesInBatch = 0;

      // go through all matching cells and delete them in batches.
      while ((rowValues = scanner.next()) != null) {
        byte[] row = rowValues.getFirst();
        for (byte[] column : rowValues.getSecond().keySet()) {
          addToDeleteBatch(batch, deleteIterator, row, column);
          deletesInBatch++;

          // perform the deletes when we have built up a batch.
          if (deletesInBatch >= deletesPerRound) {
            // delete all the entries that were found
            db.write(batch, getWriteOptions());
            batch = db.createWriteBatch();
            deletesInBatch = 0;
          }
        }
      }

      // perform any outstanding deletes
      if (deletesInBatch > 0) {
        db.write(batch, getWriteOptions());
      }
    } finally {
      scanner.close();
      deleteIterator.close();
    }
  }

  private void deleteColumn(byte[] row, byte[] column) throws IOException {
    DB db = getDB();
    WriteBatch batch = db.createWriteBatch();
    DBIterator iterator = db.iterator();
    try {
      addToDeleteBatch(batch, iterator, row, column);
      db.write(batch);
    } finally {
      iterator.close();
    }
  }

  /**
   * Helper to add deletes to a batch.  The expected use case is for the caller to be iterating
   * through leveldb keys in sorted order, collecting key values to delete in batch.
   */
  private void addToDeleteBatch(WriteBatch batch, DBIterator iterator, byte[] row, byte[] column) {
    byte[] endKey = createStartKey(row, Bytes.add(column, new byte[] { 0 }));
    iterator.seek(createStartKey(row, column));
    while (iterator.hasNext()) {
      Map.Entry<byte[], byte[]> entry = iterator.next();
      if (KeyValue.KEY_COMPARATOR.compare(entry.getKey(), endKey) >= 0) {
        // iterator is past column
        break;
      }
      batch.delete(entry.getKey());
    }
  }

  private void seekToStart(DBIterator iterator, byte[] startRow) {
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
  }


  /**
   * A scanner for a range of rows.
   */
  private static class LevelDBScanner implements Scanner {

    private final Transaction tx;
    private byte[] endKey;
    private final DBIterator iterator;
    private final byte[][] columns;
    private final FuzzyRowFilter filter;

    public LevelDBScanner(DBIterator iterator, byte[] endKey,
                          @Nullable FuzzyRowFilter filter, @Nullable byte[][] columns, @Nullable Transaction tx) {
      this.tx = tx;
      this.endKey = endKey;
      this.iterator = iterator;
      this.filter = filter;
      this.columns = columns;
    }

    @Override
    public ImmutablePair<byte[], Map<byte[], byte[]>> next() {
      try {
        while (true) {
          ImmutablePair<byte[], NavigableMap<byte[], byte[]>> result = getRow(iterator, endKey, tx, true, columns, -1);
          if (result.getFirst() == null) {
            return null;
          }
          // apply row filter if any
          if (filter != null) {
            FuzzyRowFilter.ReturnCode code = filter.filterRow(result.getFirst());
            switch (code) {
              case DONE: {
                return null;
              }
              case SEEK_NEXT_USING_HINT: {
                // row does not match but another one could. seek to next possible matching row and iterate
                byte[] seekToRow = filter.getNextRowHint(result.getFirst());
                iterator.seek(createStartKey(seekToRow));
                continue;
              }
              case INCLUDE: {
                break;
              }
            }
          }
          return ImmutablePair.of(result.getFirst(), (Map<byte[], byte[]>) result.getSecond());
        }
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
