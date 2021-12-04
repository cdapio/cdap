/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.data2.dataset2.lib.table.leveldb;

import com.google.common.base.Throwables;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.table.Result;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import org.apache.tephra.Transaction;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
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
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 * Provides common operations for levelDB tables and queues.
 */
public class LevelDBTableCore {

  private static final Logger LOG = LoggerFactory.getLogger(LevelDBTableCore.class);

  private static final Scanner EMPTY_SCANNER = createEmptyScanner();

  // this represents deleted values
  private static final byte[] DELETE_MARKER = { };

  // we use the empty column family for all data
  private static final byte[] DATA_COLFAM = { };

  // we will never write this, but use it as an upper bound for scans
  private static final byte[] NEXT_COLFAM = { 0x00 };

  // used for obtaining the next row/column for upper bound
  private static final byte[] ONE_ZERO = { 0x00 };

  private static byte[] upperBound(byte[] column) {
    return Bytes.add(column, ONE_ZERO);
  }

  private final String tableName;
  private final LevelDBTableService service;

  public LevelDBTableCore(String tableName, LevelDBTableService service) {
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
      persist(Collections.singletonMap(row, Collections.singletonMap(column, newValue)), Long.MAX_VALUE);
    }
    return true;
  }

  public synchronized Map<byte[], Long> increment(byte[] row, Map<byte[], Long> increments) throws IOException {
    Map<byte[], Long> result = new TreeMap<>(Bytes.BYTES_COMPARATOR);

    DB db = getDB();
    WriteBatch writeBatch = db.createWriteBatch();
    try (Snapshot snapshot = db.getSnapshot()) {
      ReadOptions readOptions = new ReadOptions().snapshot(snapshot);

      for (Map.Entry<byte[], Long> entry : increments.entrySet()) {
        byte[] rowKey = createPutKey(row, entry.getKey(), Long.MAX_VALUE);
        byte[] existingValue = db.get(rowKey, readOptions);
        long newValue = incrementValue(entry.getValue(), existingValue, row, entry.getKey());
        result.put(entry.getKey(), newValue);
        writeBatch.put(rowKey, Bytes.toBytes(newValue));
      }
      db.write(writeBatch, service.getWriteOptions());
    }

    return result;
  }


  public synchronized void increment(NavigableMap<byte[], NavigableMap<byte[], Long>> updates) throws IOException {
    if (updates.isEmpty()) {
      return;
    }

    DB db = getDB();
    WriteBatch writeBatch = db.createWriteBatch();
    try (Snapshot snapshot = db.getSnapshot()) {
      ReadOptions readOptions = new ReadOptions().snapshot(snapshot);

      for (Map.Entry<byte[], NavigableMap<byte[], Long>> updateEntry : updates.entrySet()) {
        for (Map.Entry<byte[], Long> entry : updateEntry.getValue().entrySet()) {
          byte[] rowKey = createPutKey(updateEntry.getKey(), entry.getKey(), Long.MAX_VALUE);
          byte[] existingValue = db.get(rowKey, readOptions);
          long newValue = incrementValue(entry.getValue(), existingValue, updateEntry.getKey(), entry.getKey());
          writeBatch.put(rowKey, Bytes.toBytes(newValue));
        }
      }
      db.write(writeBatch, service.getWriteOptions());
    }
  }

  private long incrementValue(long value, @Nullable byte[] existingValue, byte[] row, byte[] col) {
    if (existingValue == null) {
      return value;
    }
    if (existingValue.length != Bytes.SIZEOF_LONG) {
      throw new NumberFormatException("Attempted to increment a value that is not convertible to long," +
                                        " row: " + Bytes.toStringBinary(row) +
                                        " column: " + Bytes.toStringBinary(col));
    }
    return value + Bytes.toLong(existingValue);
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

  /**
   * Write the value at the target row and column with the max version {@link KeyValue.LATEST_TIMESTAMP}.
   * as a result it hides any value written with equal or smaller version.
   */
  public void putDefaultVersion(byte[] row, byte[] column, byte[] value) throws IOException {
    getDB().put(createPutKey(row, column, KeyValue.LATEST_TIMESTAMP), value);
  }

  /**
   * Write the value at the target row and column with the specified version.
   */
  public void put(byte[] row, byte[] column, byte[] value, long version) throws IOException {
    getDB().put(createPutKey(row, column, version), value);
  }

  /**
   * Read the value at the target row and column with the max version {@link KeyValue.LATEST_TIMESTAMP}.
   */
  @Nullable
  public byte[] getDefaultVersion(byte[] row, byte[] column) throws IOException {
    return getDB().get(createPutKey(row, column, KeyValue.LATEST_TIMESTAMP));
  }

  /**
   * Read the value at the target row and column with the specified version.
   */
  @Nullable
  public byte[] get(byte[] row, byte[] column, long version) throws IOException {
    return getDB().get(createPutKey(row, column, version));
  }

  /**
   * Read the latest (i.e. highest version) value at the target row and column.
   * When transaction is provided, the version returned is the latest committed value.
   */
  @Nullable
  public byte[] getLatest(byte[] row, byte[] col, @Nullable Transaction tx) throws IOException {
    byte[] startKey = createStartKey(row, col);
    byte[] endKey = createEndKey(row, upperBound(col));
    byte[] val = null;
    try (DBIterator iterator = getDB().iterator()) {
      iterator.seek(startKey);
      while (iterator.hasNext()) {
        Map.Entry<byte[], byte[]> entry = iterator.next();

        // If we have reached past the endKey, nothing is found. Break out of the loop.
        if (KeyValue.KEY_COMPARATOR.compare(entry.getKey(), endKey) >= 0) {
          break;
        }

        KeyValue kv = KeyValue.fromKey(entry.getKey());

        // Determine if this KV is visible
        if (tx != null && !tx.isVisible(kv.getTimestamp())) {
          continue;
        }

        val = entry.getValue();
        break;
      }
    }
    return val;
  }

  public void undo(Map<byte[], ? extends Map<byte[], ?>> persisted, long version) throws IOException {
    if (persisted.isEmpty()) {
      return;
    }
    DB db = getDB();
    WriteBatch batch = db.createWriteBatch();
    for (Map.Entry<byte[], ? extends Map<byte[], ?>> row : persisted.entrySet()) {
      for (Map.Entry<byte[], ?> column : row.getValue().entrySet()) {
        byte[] key = createPutKey(row.getKey(), column.getKey(), version);
        batch.delete(key);
      }
    }
    db.write(batch, service.getWriteOptions());
  }

  public Scanner scan(byte[] startRow, byte[] stopRow,
                      @Nullable FuzzyRowFilter filter, @Nullable byte[][] columns, @Nullable Transaction tx)
    throws IOException {
    if (columns != null) {
      if (columns.length == 0) {
        return EMPTY_SCANNER;
      }
      columns = Arrays.copyOf(columns, columns.length);
      Arrays.sort(columns, Bytes.BYTES_COMPARATOR);
    }

    DBIterator iterator = getDB().iterator();
    seekToStart(iterator, startRow);
    byte[] endKey = stopRow == null ? null : createStartKey(stopRow);
    return new LevelDBScanner(iterator, endKey, filter, columns, tx);
  }

  /**
   * if columns are not null, then limit param is ignored and limit is columns.length
   */
  public NavigableMap<byte[], byte[]> getRow(byte[] row, @Nullable byte[][] columns,
                                             @Nullable byte[] startCol, @Nullable byte[] stopCol,
                                             int limit, @Nullable Transaction tx) throws IOException {
    if (columns != null) {
      if (columns.length == 0) {
        return Collections.emptyNavigableMap();
      }
      columns = Arrays.copyOf(columns, columns.length);
      Arrays.sort(columns, Bytes.BYTES_COMPARATOR);
      limit = columns.length;
    }

    byte[] startKey = createStartKey(row, columns == null ? startCol : columns[0]);
    byte[] endKey = createEndKey(row, columns == null ? stopCol : upperBound(columns[columns.length - 1]));
    try (DBIterator iterator = getDB().iterator()) {
      iterator.seek(startKey);
      return getRow(iterator, endKey, tx, false, columns, limit).getSecond();
    }
  }

  private static Scanner createEmptyScanner() {
    return new Scanner() {
      @Override
      public Row next() {
        return null;
      }

      @Override
      public void close() {
        // no-op
      }
    };
  }


  /**
   * Read one row of the table at the latest or highest version. This is used both by getRow() and by Scanner.next().
   * @param iterator An iterator over the database. This is passed in such that the caller can reuse the same
   *                 iterator if scanning multiple rows.
   * @param endKey An upper bound for the (leveldb) keys to read. This method never reads past that key.
   * @param tx The transaction to use for visibility.
   * @param multiRow If true indicates that the row may end before the endKey. In that case,
   *                 this method will stop reading as soon as it sees more than one row key. The iterator will not be
   *                 advanced past the beginning of the next row (so that next time, we still see the entire next row).
   * @param columns If non-null, only columns contained in this will be returned. The given columns should be sorted.
   * @param limit If non-negative, at most this many columns will be returned. If multiRow is true, this is ignored.
   * @return a pair consisting of the row key of the next non-empty row and the column map for that row. If multiRow
   *         is false, null is returned for row key because the caller already knows it.
   */
  private static ImmutablePair<byte[], NavigableMap<byte[], byte[]>> getRow(DBIterator iterator,
                                                                            @Nullable byte[] endKey,
                                                                            @Nullable Transaction tx,
                                                                            boolean multiRow,
                                                                            @Nullable byte[][] columns, int limit) {
    byte[] rowBeingRead = null;
    byte[] previousRow = null;
    byte[] previousCol = null;
    NavigableMap<byte[], byte[]> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);

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
    return new ImmutablePair<>(rowBeingRead, map);
  }

  /**
   * Delete the cell at specified row and column with max version {@link KeyValue.LATEST_TIMESTAMP}.
   */
  public void deleteDefaultVersion(byte[] row, byte[] column) throws IOException {
    getDB().delete(createPutKey(row, column, KeyValue.LATEST_TIMESTAMP));
  }

  /**
   * Delete the cell at specified row and column with specified version.
   */
  public void delete(byte[] row, byte[] column, long version) throws IOException {
    getDB().delete(createPutKey(row, column, version));
  }

  /**
   * Delete a list of rows from the table entirely, disregarding transactions.
   * This includes all columns and all versions of cells for each specified row.
   *
   * Note that this operation could be quite slow when there are large number of columns
   * or versions for cells in these roles. Moreover, such deletions are converted to
   * tombstones or deletion-markers that can slow subsequent scan operations over
   * overlapping ranges, thus leading to performance issues, unless these deletions
   * are compacted away, so be cautious when calling this function over a larger number of
   * rows or rows with wide columns and large number of versions.
   *
   * @param toDelete the row keys to delete
   *
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
    WriteBatch batch = db.createWriteBatch();
    try (DBIterator iterator = db.iterator()) {
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
        } else {
          // iterator must seek to current row
          iterator.seek(createStartKey(currentRow));
          entry = iterator.hasNext() ? iterator.next() : null;
        }
      }
    }
    // delete all the entries that were found
    db.write(batch, getWriteOptions());
  }

  public void deleteRange(byte[] startRow, byte[] stopRow, @Nullable FuzzyRowFilter filter, @Nullable byte[][] columns)
    throws IOException {
    if (columns != null) {
      if (columns.length == 0) {
        return;
      }
      columns = Arrays.copyOf(columns, columns.length);
      Arrays.sort(columns, Bytes.BYTES_COMPARATOR);
    }

    DB db = getDB();
    DBIterator iterator = db.iterator();
    seekToStart(iterator, startRow);
    byte[] endKey = stopRow == null ? null : createStartKey(stopRow);

    DBIterator deleteIterator = db.iterator();
    seekToStart(deleteIterator, startRow);
    final int deletesPerRound = 1024; // todo make configurable
    try (Scanner scanner = new LevelDBScanner(iterator, endKey, filter, columns, null)) {
      Row rowValues;
      WriteBatch batch = db.createWriteBatch();
      int deletesInBatch = 0;

      // go through all matching cells and delete them in batches.
      while ((rowValues = scanner.next()) != null) {
        byte[] row = rowValues.getRow();
        for (byte[] column : rowValues.getColumns().keySet()) {
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
      deleteIterator.close();
    }
  }

  public void deleteColumn(byte[] row, byte[] column) throws IOException {
    DB db = getDB();
    WriteBatch batch = db.createWriteBatch();
    try (DBIterator iterator = db.iterator()) {
      addToDeleteBatch(batch, iterator, row, column);
      db.write(batch);
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

    LevelDBScanner(DBIterator iterator, byte[] endKey,
                   @Nullable FuzzyRowFilter filter, @Nullable byte[][] columns, @Nullable Transaction tx) {
      this.tx = tx;
      this.endKey = endKey;
      this.iterator = iterator;
      this.filter = filter;
      this.columns = columns;
    }

    @Override
    public Row next() {
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
          return new Result(result.getFirst(), result.getSecond());
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
    return KeyValue.getKey(rowKey, DATA_COLFAM, columnKey, version, KeyValue.Type.Put);
  }

  private static byte[] createStartKey(byte[] row) { // the first possible key of a row
    return KeyValue.getKey(row, DATA_COLFAM, null, KeyValue.LATEST_TIMESTAMP, KeyValue.Type.Maximum);
  }

  private static byte[] createStartKey(byte[] row, byte[] column) {
    return KeyValue.getKey(row, DATA_COLFAM, column, KeyValue.LATEST_TIMESTAMP, KeyValue.Type.Maximum);
  }

  private static byte[] createEndKey(byte[] row, byte[] column) {
    if (column != null) {
      // we have a stop column and can use that as an upper bound
      return KeyValue.getKey(row, DATA_COLFAM, column, KeyValue.LATEST_TIMESTAMP, KeyValue.Type.Maximum);
    } else {
      // no stop column - use next column family as upper bound
      return KeyValue.getKey(row, NEXT_COLFAM, null, KeyValue.LATEST_TIMESTAMP, KeyValue.Type.Maximum);
    }
  }
}
