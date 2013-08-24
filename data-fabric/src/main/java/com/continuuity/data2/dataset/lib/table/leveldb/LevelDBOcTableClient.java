package com.continuuity.data2.dataset.lib.table.leveldb;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.engine.leveldb.KeyValue;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.dataset.lib.table.BackedByVersionedStoreOcTableClient;
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
import java.util.Map;
import java.util.NavigableMap;

/**
 * A table client based on LevelDB.
 */
public class LevelDBOcTableClient extends BackedByVersionedStoreOcTableClient {

  private static final Logger LOG = LoggerFactory.getLogger(LevelDBOcTableClient.class);

  // we use the empty column family for all data
  private static final byte[] DATA_COLFAM = { 'd' };

  // we will never write this, but use it as an upper bound for scans
  private static final byte[] NEXT_COLFAM = { 'd', 0x00 };

  // we can enable fsync() on every write, off for now
  private static final WriteOptions WRITE_OPTIONS = new WriteOptions().sync(false);

  private static final byte[] ONE_ZERO = { 0x00 };

  private static byte[] upperBound(byte[] column) {
    return Bytes.add(column, ONE_ZERO);
  }

  private DB db;
  private Transaction tx;
  private long persistedVersion;

  public LevelDBOcTableClient(String tableName, DB db) throws IOException {
    super(tableName);
    this.db = db;
  }

  // TODO this is the same for all OcTableClient implementations -> promote to base class
  @Override
  public void startTx(Transaction tx) {
    super.startTx(tx);
    this.tx = tx;
  }

  @Override
  protected void persist(NavigableMap<byte[], NavigableMap<byte[], byte[]>> changes) throws Exception {
    WriteBatch batch = db.createWriteBatch();
    persistedVersion = tx == null ? System.currentTimeMillis() : tx.getWritePointer();
    for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> row : changes.entrySet()) {
      for (Map.Entry<byte[], byte[]> column : row.getValue().entrySet()) {
        byte[] key = createPutKey(row.getKey(), column.getKey(), persistedVersion);
        // we want support tx and non-tx modes
        if (tx != null) {
          batch.put(key, wrapDeleteIfNeeded(column.getValue()));
        } else {
          batch.put(key, column.getValue());
        }
      }
    }
    db.write(batch, WRITE_OPTIONS);
  }

  @Override
  protected void undo(NavigableMap<byte[], NavigableMap<byte[], byte[]>> persisted) throws Exception {
    WriteBatch batch = db.createWriteBatch();
    for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> row : persisted.entrySet()) {
      for (Map.Entry<byte[], byte[]> column : row.getValue().entrySet()) {
        // delete the version that was persisted earlier
        byte[] key = createPutKey(row.getKey(), column.getKey(), persistedVersion);
        batch.delete(key);
      }
    }
    db.write(batch, WRITE_OPTIONS);
  }

  @Override
  protected byte[] getPersisted(byte[] row, byte[] column) throws Exception {
    return getInternal(row, new byte[][] { column }, null, null, 1).get(column);
  }

  @Override
  protected NavigableMap<byte[], byte[]> getPersisted(byte[] row, byte[][] columns) throws Exception {
    return getInternal(row, columns, null, null, columns.length);
  }

  @Override
  protected NavigableMap<byte[], byte[]> getPersisted(byte[] row, byte[] startColumn, byte[] stopColumn, int limit)
    throws Exception {
    return getInternal(row, null, startColumn, stopColumn, limit);
  }

  @Override
  protected Scanner scanPersisted(byte[] startRow, byte[] stopRow) throws Exception {
    DBIterator iterator = db.iterator();
    try {
      if (startRow != null) {
       iterator.seek(createStartKey(startRow));
      } else {
        iterator.seekToFirst();
      }
    } catch (Exception e) {
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

  private static boolean isVisible(Transaction transaction, long version) {
    return transaction == null ||
      version <= transaction.getReadPointer() && Arrays.binarySearch(transaction.getExcludedList(), version) < 0;
  }

  private NavigableMap<byte[], byte[]> getInternal(byte[] row, byte[][] columns,
                                                   byte[] startCol, byte[] stopCol, int limit) throws IOException {

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
   * Read one row of the table. This is used both by getPersisted() and by Scanner.next().
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
      if (!isVisible(tx, kv.getTimestamp())) {
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

  private byte[] createPutKey(byte[] rowKey, byte[] columnKey, long version) {
    return new KeyValue(rowKey, DATA_COLFAM, columnKey, version, KeyValue.Type.Put).getKey();
  }

  private byte[] createStartKey(byte[] row) { // the first possible key of a row
    return new KeyValue(row, DATA_COLFAM, null, KeyValue.LATEST_TIMESTAMP, KeyValue.Type.Maximum).getKey();
  }
  private byte[] createEndKey(byte[] row) {
    return createStartKey(row); // the first key of the stop is the first to be excluded
  }

  private byte[] createStartKey(byte[] row, byte[] column) {
    return new KeyValue(row, DATA_COLFAM, column, KeyValue.LATEST_TIMESTAMP, KeyValue.Type.Maximum).getKey();
  }

  private byte[] createEndKey(byte[] row, byte[] column) {
    if (column != null) {
      // we have a stop column and can use that as an upper bound
      return new KeyValue(row, DATA_COLFAM, column, KeyValue.LATEST_TIMESTAMP, KeyValue.Type.Maximum).getKey();
    } else {
      // no stop column - use next column family as upper bound
      return new KeyValue(row, NEXT_COLFAM, null, KeyValue.LATEST_TIMESTAMP, KeyValue.Type.Maximum).getKey();
    }
  }
}
