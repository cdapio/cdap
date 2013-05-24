package com.continuuity.data.engine.leveldb;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.table.AbstractOVCTable;
import com.continuuity.data.table.Scanner;
import com.continuuity.data.util.RowLockTable;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.Bytes;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

/**
 * Implementation of an OVCTable over a LevelDB Database.
 */
public class LevelDBOVCTable extends AbstractOVCTable {

  private static final Logger LOG = LoggerFactory.getLogger(LevelDBOVCTable.class);

  private static final String dbFilePrefix = "ldb_";

  private static final byte[] FAMILY = new byte[]{'f'};

  private static final byte[] NULL_VAL = new byte[0];

  private final String basePath;

  private final String encodedTableName;

  private final Integer blockSize;
  private final Long cacheSize;

  private DB db;

  // this will be used for row-level locking. Because the levelDB may grow very large,
  // and we want to keep the memory footprint small, we will always remove locks from
  // the table at the time we release them. This can have a slight performance overhead,
  // because other threads can get an invalid lock (see RowLockTable) and then have to
  // create a new lock. Therefore we always use validLock() to obtain a lock.
  private final RowLockTable locks = new RowLockTable();

  LevelDBOVCTable(final String basePath, final String tableName, final Integer blockSize, final Long cacheSize) {
    this.basePath = basePath;
    this.blockSize = blockSize;
    this.cacheSize = cacheSize;
    try {
      this.encodedTableName = URLEncoder.encode(tableName, "ASCII");
    } catch (UnsupportedEncodingException e) {
      LOG.error("Error encoding table name '" + tableName + "'", e);
      throw new RuntimeException(e);
    }
  }

  private String generateDBPath() {
    return basePath + System.getProperty("file.separator") +
      dbFilePrefix + encodedTableName;
  }

  private Options generateDBOptions(boolean createIfMissing, boolean errorIfExists) {
    Options options = new Options();
    options.createIfMissing(createIfMissing);
    options.errorIfExists(errorIfExists);
    options.comparator(new KeyValueDBComparator());
    options.blockSize(blockSize);
    options.cacheSize(cacheSize);
    return options;
  }

  synchronized boolean openTable() throws OperationException {
    try {
      this.db = factory.open(new File(generateDBPath()), generateDBOptions(false, false));
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  synchronized void initializeTable() throws OperationException {
    try {
      this.db = factory.open(new File(generateDBPath()), generateDBOptions(true, false));
    } catch (IOException e) {
      throw createOperationException(e, "create");
    }
  }

  /**
   * A comparator for the keys of HBase key/value pairs.
   */
  public static class KeyValueDBComparator implements DBComparator {

    @Override
    public int compare(byte[] left, byte[] right) {
      return KeyValue.KEY_COMPARATOR.compare(left, right);
    }

    @Override
    public byte[] findShortSuccessor(byte[] key) {
      return key;
    }

    @Override
    public byte[] findShortestSeparator(byte[] start, byte[] limit) {
      return start;
    }

    @Override
    public String name() {
      return "hbase-kv";
    }

  }

  // LevelDB specific helpers

  private byte[] createStartKey(byte[] row) {
    return new KeyValue(row, FAMILY, null, HConstants.LATEST_TIMESTAMP, Type.Maximum).getKey();
  }

  private byte[] createStartKey(byte[] row, byte[] column) {
    return new KeyValue(row, FAMILY, column, HConstants.LATEST_TIMESTAMP, Type.Maximum).getKey();
  }

  private byte[] createEndKey(byte[] row) {
    return new KeyValue(row, null, null, HConstants.LATEST_TIMESTAMP, Type.Minimum).getKey();
  }

  private byte[] createEndKey(byte[] row, byte[] column) {
    return new KeyValue(row, FAMILY, column, 0L, Type.Minimum).getKey();
  }

  private byte[] appendByte(final byte[] value, byte b) {
    byte[] newValue = new byte[value.length + 1];
    System.arraycopy(value, 0, newValue, 0, value.length);
    newValue[value.length] = b;
    return newValue;
  }

  private KeyValue readKeyValueRangeAndGetLatest(byte[] row, byte[] column, ReadPointer readPointer)
    throws DBException, IOException {
    DBIterator iterator = db.iterator();
    try {
      byte[] startKey = createStartKey(row, column);
      byte[] endKey = createEndKey(row, column);
      long lastDelete = -1;
      long undeleted = -1;
      for (iterator.seek(startKey); iterator.hasNext(); iterator.next()) {
        byte[] key = iterator.peekNext().getKey();
        byte[] value = iterator.peekNext().getValue();
        // If we have reached past the endKey, nothing was found, return null

        if (KeyValue.KEY_COMPARATOR.compare(key, endKey) >= 0) {
          return null;
        }

        KeyValue kv = createKeyValue(key, value);
        long curVersion = kv.getTimestamp();

        // Determine if this KV is visible
        if (!readPointer.isVisible(curVersion)) {
          continue;
        }
        Type type = Type.codeToType(kv.getType());

        if (type == Type.Delete) {
          lastDelete = curVersion;
        } else if (type == Type.UndeleteColumn) {
          undeleted = curVersion;
        } else if (type == Type.DeleteColumn) {
          if (undeleted != curVersion) {
            break;
          }
        } else if (type == Type.Put) {
          if (curVersion != lastDelete) {
            // If we get here, this version is visible
            return kv;
          }
        }
      }
    } finally {
      iterator.close();
    }
    // Nothing found
    return null;
  }

  private Map<byte[], byte[]> readKeyValueRangeAndGetLatest(byte[] row, ReadPointer readPointer)
    throws DBException, IOException {
    return readKeyValueRangeAndGetLatest(row, null, null, readPointer, -1);
  }

  private Map<byte[], byte[]> readKeyValueRangeAndGetLatest(byte[] row, byte[][] columns, ReadPointer readPointer)
    throws DBException, IOException {
    Map<byte[], byte[]> map = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    byte[][] orderedColumns = Arrays.copyOf(columns, columns.length);
    Arrays.sort(orderedColumns, Bytes.BYTES_COMPARATOR);
    byte[] startKey = createStartKey(row, orderedColumns[0]);
    byte[] endKey = createEndKey(row, orderedColumns[columns.length - 1]);
    DBIterator iterator = db.iterator();
    int colIdx = 0;
    long lastDelete = -1;
    long undeleted = -1;

    try {
      for (iterator.seek(startKey); iterator.hasNext(); ) {
        byte[] key = iterator.peekNext().getKey();
        byte[] value = iterator.peekNext().getValue();
        // If we have reached past the endKey, nothing was found, return null

        if (KeyValue.KEY_COMPARATOR.compare(key, endKey) >= 0) {
          return map;
        }
        KeyValue kv = createKeyValue(key, value);
        long curVersion = kv.getTimestamp();

        // Determine if this KV is visible
        if (!readPointer.isVisible(curVersion)) {
          iterator.next();
          continue;
        }
        Type type = Type.codeToType(kv.getType());

        if (type == Type.Delete) {
          //delete of one version
          lastDelete = curVersion;
          iterator.next();
        } else if (type == Type.UndeleteColumn) {
          undeleted = curVersion;
          iterator.next();
        } else if (type == Type.DeleteColumn) {
          //delete of entire column (all cells)
          if (undeleted != curVersion) {
            if (colIdx == orderedColumns.length - 1) {
              break;
            }
            iterator.seek(createStartKey(row, orderedColumns[++colIdx]));
          } else {
            iterator.next();
          }
        } else if (type == Type.Put) {
          if (curVersion != lastDelete) {
            // If we get here, this version is visible
            map.put(kv.getQualifier(), kv.getValue());
            if (colIdx == orderedColumns.length - 1) {
              break;
            }
            iterator.seek(createStartKey(row, orderedColumns[++colIdx]));
          } else {
            iterator.next();
          }
        }
      }
    } finally {
      iterator.close();
    }
    return map;
  }

  private byte[] getNextLexicographicalQualifier(byte[] qualifier) {
    //appending 0x00 to current qualifier gives you next possible lexicographical
    return appendByte(qualifier, (byte) 0x00);
  }

  private Map<byte[], byte[]> readKeyValueRangeAndGetLatest(byte[] row, byte[] startColumn, byte[] stopColumn,
                                                            ReadPointer readPointer, int limit)
    throws DBException, IOException {
    // negative limit means unlimited results
    if (limit <= 0) {
      limit = Integer.MAX_VALUE;
    }

    byte[] startKey = startColumn == null ? createStartKey(row) : createStartKey(row, startColumn);
    byte[] endKey = stopColumn == null ? createEndKey(row) : createEndKey(row, stopColumn);

    Map<byte[], byte[]> map = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    DBIterator iterator = db.iterator();
    byte[] prevColumn = null;
    long lastDelete = -1;
    long undeleted = -1;

    try {
      for (iterator.seek(startKey); iterator.hasNext(); ) {
        byte[] key = iterator.peekNext().getKey();
        byte[] value = iterator.peekNext().getValue();
        // If we have reached past the endKey, nothing was found, return null
        KeyValue kv = createKeyValue(key, value);
        long curVersion = kv.getTimestamp();
        byte[] curColumn = kv.getQualifier();

        if ((Bytes.equals(curColumn, stopColumn)) || (KeyValue.KEY_COMPARATOR.compare(key, endKey) >= 0)) {
          return map;
        }

        // Determine if this KV is visible
        if (!readPointer.isVisible(curVersion)) {
          prevColumn = curColumn;
          iterator.next();
          continue;
        }
        Type type = Type.codeToType(kv.getType());

        if (type == Type.Delete) {
          //delete of one version
          lastDelete = curVersion;
          prevColumn = curColumn;
          iterator.next();
        } else if (type == Type.UndeleteColumn) {
          undeleted = curVersion;
          prevColumn = null;
          iterator.next();
        } else if (type == Type.DeleteColumn) {
          //delete of entire column (all cells)
          if (undeleted == curVersion) {
            iterator.next();
          } else {
            lastDelete = -1;
            undeleted = -1;
            iterator.seek(createStartKey(row, getNextLexicographicalQualifier(curColumn)));
          }
          prevColumn = null;
        } else if (type == Type.Put) {
          if ((curVersion == lastDelete) && (Bytes.equals(prevColumn, curColumn))) {
            prevColumn = curColumn;
            iterator.next();
          } else {
            // If we get here, this version is visible
            map.put(curColumn, kv.getValue());
            // break out if limit reached
            if (map.size() == limit) {
              break;
            }
            prevColumn = key;
            lastDelete = -1;
            undeleted = -1;
            iterator.seek(createStartKey(row, getNextLexicographicalQualifier(curColumn)));
          }
        }
      }
    } finally {
      iterator.close();
    }
    return map;
  }

  private KeyValue createKeyValue(byte[] key, byte[] value) {
    int len = key.length + value.length + (2 * Bytes.SIZEOF_INT);
    byte[] kvBytes = new byte[len];
    int pos = 0;
    pos = Bytes.putInt(kvBytes, pos, key.length);
    pos = Bytes.putInt(kvBytes, pos, value.length);
    pos = Bytes.putBytes(kvBytes, pos, key, 0, key.length);
    Bytes.putBytes(kvBytes, pos, value, 0, value.length);
    return new KeyValue(kvBytes);
  }

  // Administrative Operations

  @Override
  public synchronized void clear() throws OperationException {
    try {
      db.close();
      factory.destroy(new File(generateDBPath()), new Options());
    } catch (IOException e) {
      throw createOperationException(e, "clearing");
    }
    initializeTable();
  }

  // Simple Write Operations

  @Override
  public void put(byte[] row, byte[] column, long version, byte[] value) throws OperationException {
    performInsert(row, column, version, Type.Put, value);
  }

  @Override
  public void put(byte[] row, byte[][] columns, long version, byte[][] values) throws OperationException {
    performInsert(row, columns, version, Type.Put, values);
  }

  @Override
  public void put(byte[][] rows, byte[][] columns, long version, byte[][] values) throws OperationException {
    assert (rows.length == columns.length);
    assert (rows.length == values.length);

    for (int i = 0; i < rows.length; ++i) {
      performInsert(rows[i], columns[i], version, Type.Put, values[i]);
    }
  }

  @Override
  public void put(byte[][] rows, byte[][][] columnsPerRow, long version, byte[][][] valuesPerRow)
    throws OperationException {
    for (int i = 0; i < rows.length; i++) {
      performInsert(rows[i], columnsPerRow[i], version, Type.Put, valuesPerRow[i]);
    }
  }

// Delete Operations

  @Override
  public void delete(byte[] row, byte[] column, long version) throws OperationException {
    performInsert(row, column, version, Type.Delete, NULL_VAL);
  }

  @Override
  public void delete(byte[] row, byte[][] columns, long version) throws OperationException {
    performInsert(row, columns, version, Type.Delete, generateDeleteVals(columns.length));
  }

  @Override
  public void deleteAll(byte[] row, byte[] column, long version) throws OperationException {
    performInsert(row, column, version, Type.DeleteColumn, NULL_VAL);
  }

  @Override
  public void deleteAll(byte[] row, byte[][] columns, long version) throws OperationException {
    performInsert(row, columns, version, Type.DeleteColumn, generateDeleteVals(columns.length));
  }

  @Override
  public void deleteDirty(byte[] row, byte[][] columns, long version) throws OperationException {
    deleteAll(row, columns, version);
  }

  @Override
  public void deleteDirty(byte[][] rows) throws OperationException {
    try {
      for (byte[] row : rows) {
        byte[] startKey = createStartKey(row);
        byte[] endKey = createEndKey(row);
        DBIterator iterator = db.iterator();
        iterator.seek(startKey);
        while (iterator.hasNext()) {
          byte[] nextKey = iterator.next().getKey();
          if (KeyValue.KEY_COMPARATOR.compare(nextKey, endKey) > 0) {
            break;
          }
          db.delete(nextKey);
        }
      }
    } catch (DBException dbe) {
      throw createOperationException(dbe, "delete");
    }
  }

  @Override
  public void undeleteAll(byte[] row, byte[] column, long version) throws OperationException {
    performInsert(row, column, version, Type.UndeleteColumn, NULL_VAL);
  }

  @Override
  public void undeleteAll(byte[] row, byte[][] columns, long version) throws OperationException {
    performInsert(row, columns, version, Type.UndeleteColumn, generateDeleteVals(columns.length));
  }

  private byte[][] generateDeleteVals(int length) {
    byte[][] values = new byte[length][];
    for (int i = 0; i < values.length; i++) {
      values[i] = NULL_VAL;
    }
    return values;
  }


  // Read Operations

  @Override
  public OperationResult<Map<byte[], byte[]>> get(byte[] row, ReadPointer readPointer) throws OperationException {
    try {
      Map<byte[], byte[]> latest = readKeyValueRangeAndGetLatest(row, readPointer);
      if (latest.isEmpty()) {
        return new OperationResult<Map<byte[], byte[]>>(StatusCode.KEY_NOT_FOUND);
      }
      return new OperationResult<Map<byte[], byte[]>>(latest);
    } catch (IOException e) {
      throw createOperationException(e, "get");
    }
  }

  @Override
  public OperationResult<byte[]> get(byte[] row, byte[] column, ReadPointer readPointer) throws OperationException {
    OperationResult<ImmutablePair<byte[], Long>> res = getWithVersion(row, column, readPointer);
    if (res.isEmpty()) {
      return new OperationResult<byte[]>(res.getStatus(), res.getMessage());
    } else {
      return new OperationResult<byte[]>(res.getValue().getFirst());
    }
  }

  @Override
  public OperationResult<byte[]> getDirty(byte[] row, byte[] column) throws OperationException {
    return get(row, column, TransactionOracle.DIRTY_READ_POINTER);
  }

  @Override
  public OperationResult<ImmutablePair<byte[], Long>> getWithVersion(byte[] row, byte[] column, ReadPointer readPointer)
    throws OperationException {
    try {
      KeyValue latest = readKeyValueRangeAndGetLatest(row, column, readPointer);
      if (latest == null) {
        return new OperationResult<ImmutablePair<byte[], Long>>(StatusCode.COLUMN_NOT_FOUND);
      }

      return new OperationResult<ImmutablePair<byte[], Long>>(new ImmutablePair<byte[], Long>(latest.getValue(),
                                                                                              latest.getTimestamp()));

    } catch (IOException e) {
      throw createOperationException(e, "get");
    }
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> get(byte[] row, final byte[] startColumn, final byte[] stopColumn,
                                                  int limit, ReadPointer readPointer)
    throws OperationException {
    try {
      Map<byte[], byte[]> latest = readKeyValueRangeAndGetLatest(row, startColumn, stopColumn, readPointer, limit);

      if (latest == null || latest.isEmpty()) {
        return new OperationResult<Map<byte[], byte[]>>(StatusCode.COLUMN_NOT_FOUND);
      }
      return new OperationResult<Map<byte[], byte[]>>(latest);
    } catch (IOException e) {
      throw createOperationException(e, "get");
    }
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> get(byte[] row, byte[][] columns, ReadPointer readPointer)
    throws OperationException {
    try {
      Map<byte[], byte[]> map = readKeyValueRangeAndGetLatest(row, columns, readPointer);

      if (map == null || map.isEmpty()) {

        return new OperationResult<Map<byte[], byte[]>>(StatusCode.COLUMN_NOT_FOUND);
      }
      return new OperationResult<Map<byte[], byte[]>>(map);
    } catch (IOException e) {
      throw createOperationException(e, "get");
    }
  }

  @Override
  public OperationResult<Map<byte[], Map<byte[], byte[]>>> getAllColumns(byte[][] rows, byte[][] columns,
                                                                         ReadPointer readPointer)
    throws OperationException {
    // TODO: can the below algorithm be improved by doing something like a scan of rows instead of point lookups?
    Map<byte[], Map<byte[], byte[]>> retMap = new TreeMap<byte[], Map<byte[], byte[]>>(Bytes.BYTES_COMPARATOR);
    try {
      for (byte[] row : rows) {
        Map<byte[], byte[]> map = readKeyValueRangeAndGetLatest(row, columns, readPointer);
        if (map != null) {
          retMap.put(row, map);
        }
      }
      // Remove empty rows
      Iterator<Map.Entry<byte[], Map<byte[], byte[]>>> iterator = retMap.entrySet().iterator();
      while (iterator.hasNext()) {
        if (iterator.next().getValue().isEmpty()) {
          iterator.remove();
        }
      }
      if (retMap.isEmpty()) {
        return new OperationResult<Map<byte[], Map<byte[], byte[]>>>(StatusCode.KEY_NOT_FOUND);
      } else {
        return new OperationResult<Map<byte[], Map<byte[], byte[]>>>(retMap);
      }
    } catch (IOException e) {
      throw createOperationException(e, "get");
    }
  }

  // Scan Operations

  @Override
  public List<byte[]> getKeys(int limit, int offset, ReadPointer readPointer) throws OperationException {
    DBIterator iterator = db.iterator();

    try {
      List<KeyValue> kvs = new ArrayList<KeyValue>();
      for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
        byte[] key = iterator.peekNext().getKey();
        byte[] value = iterator.peekNext().getValue();
        kvs.add(createKeyValue(key, value));
      }

      List<byte[]> keys = new ArrayList<byte[]>(limit > 1024 ? 1024 : limit);
      int returned = 0;
      int skipped = 0;
      long lastDelete = -1;
      long undeleted = -1;
      byte[] lastRow = new byte[0];
      byte[] curRow = new byte[0];
      byte[] curCol = new byte[0];
      byte[] lastCol = new byte[0];
      for (KeyValue kv : kvs) {
        if (returned >= limit) {
          break;
        }

        // See if we already included this row
        byte[] row = kv.getRow();
        if (Bytes.equals(lastRow, row)) {
          continue;
        }

        // See if this is a new row (clear col/del tracking if so)
        if (!Bytes.equals(curRow, row)) {
          lastCol = new byte[0];
          curCol = new byte[0];
          lastDelete = -1;
          undeleted = -1;
        }
        curRow = row;

        // Check visibility of this entry
        long curVersion = kv.getTimestamp();
        // Check if this entry is visible, skip if not
        if (!readPointer.isVisible(curVersion)) {
          continue;
        }

        byte[] column = kv.getQualifier();
        // Check if this column has been completely deleted
        if (Bytes.equals(lastCol, column)) {
          continue;
        }
        // Check if this is a new column, reset delete pointers if so
        if (!Bytes.equals(curCol, column)) {
          curCol = column;
          lastDelete = -1;
          undeleted = -1;
        }
        // Check if type is a delete and execute accordingly
        Type type = Type.codeToType(kv.getType());
        if (type == Type.UndeleteColumn) {
          undeleted = curVersion;
          continue;
        }
        if (type == Type.DeleteColumn) {
          if (undeleted == curVersion) {
            continue;
          } else {
            // The rest of this column has been deleted, act like we returned it
            lastCol = column;
            continue;
          }
        }
        if (type == Type.Delete) {
          lastDelete = curVersion;
          continue;
        }
        if (curVersion == lastDelete) {
          continue;
        }
        // Column is valid, therefore row is valid, add row
        lastRow = row;
        if (skipped < offset) {
          skipped++;
        } else {
          keys.add(row);
          returned++;
        }
      }
      return keys;
    } finally {
      try {
        iterator.close();
      } catch (IOException e) {
        throw createOperationException(e, "closing iterator");
      }
    }
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow, ReadPointer readPointer) {
    return new LevelDBScanner(db.iterator(), startRow, stopRow, readPointer);
  }

  // Private Helper Methods

  private void performInsert(byte[] row, byte[] column, long version, Type type, byte[] value)
    throws OperationException {
    KeyValue kv = new KeyValue(row, FAMILY, column, version, type, value);
    try {
      WriteOptions options = new WriteOptions();
      // options.sync(true); We can enable fsync() on every write, off for now
      db.put(kv.getKey(), kv.getValue(), options);
    } catch (DBException dbe) {
      throw createOperationException(dbe, "insert");
    }
  }

  private void performInsert(byte[] row, byte[][] columns, long version, Type type, byte[][] values)
    throws OperationException {
    for (int i = 0; i < columns.length; i++) {
      performInsert(row, columns[i], version, type, values[i]);
    }
  }

  /**
   * Result has (column, version, kvtype, id, value).
   *
   * @throws DBException
   */

  // Read-Modify-Write Operations
  private long internalIncrement(byte[] row, byte[] column, long amount, ReadPointer readPointer, long writeVersion)
    throws OperationException {
    long newAmount = amount;
    // Read existing value
    OperationResult<byte[]> readResult = get(row, column, readPointer);
    if (!readResult.isEmpty()) {
      try {
        newAmount += Bytes.toLong(readResult.getValue());
      } catch (IllegalArgumentException e) {
        throw new OperationException(StatusCode.ILLEGAL_INCREMENT, e.getMessage(), e);
      }
    }
    // Write new value
    performInsert(row, column, writeVersion, Type.Put, Bytes.toBytes(newAmount));
    return newAmount;
  }

  @Override
  public long increment(byte[] row, byte[] column, long amount, ReadPointer readPointer, long writeVersion)
    throws OperationException {
    RowLockTable.Row r = new RowLockTable.Row(row);
    this.locks.validLock(r);
    long newAmount;
    try {
      newAmount = internalIncrement(row, column, amount, readPointer, writeVersion);
    } finally {
      this.locks.unlockAndRemove(r);
    }
    return newAmount;
  }

  @Override
  public Map<byte[], Long> increment(byte[] row, byte[][] columns, long[] amounts, ReadPointer readPointer,
                                     long writeVersion)
    throws OperationException {
    RowLockTable.Row r = new RowLockTable.Row(row);
    this.locks.validLock(r);
    Map<byte[], Long> ret = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
    try {
      for (int i = 0; i < columns.length; i++) {
        ret.put(columns[i], internalIncrement(row, columns[i], amounts[i], readPointer, writeVersion));
      }
    } finally {
      this.locks.unlockAndRemove(r);
    }
    return ret;
  }

  @Override
  public long incrementAtomicDirtily(byte[] row, byte[] column, long amount) throws OperationException {
    return increment(row, column, amount, TransactionOracle.DIRTY_READ_POINTER, TransactionOracle.DIRTY_WRITE_VERSION);
  }

  @Override
  public void compareAndSwap(byte[] row, byte[] column, byte[] expectedValue, byte[] newValue,
                             ReadPointer readPointer, long writeVersion)
    throws OperationException {

    RowLockTable.Row r = new RowLockTable.Row(row);
    this.locks.validLock(r);
    try {
      // Read existing value
      OperationResult<byte[]> readResult = get(row, column, readPointer);
      byte[] existingValue = readResult.getValue();

      // Handle cases regarding non-existent values
      if (existingValue == null && expectedValue != null) {
        throw new OperationException(StatusCode.WRITE_CONFLICT, "CompareAndSwap expected value mismatch");
      }
      if (existingValue != null && expectedValue == null) {
        throw new OperationException(StatusCode.WRITE_CONFLICT, "CompareAndSwap expected value mismatch");
      }

      // if nothing existed, write data
      if (expectedValue == null) {
        performInsert(row, column, writeVersion, Type.Put, newValue);
        return;
      }

      // check if expected == existing, fail if not
      if (!Bytes.equals(expectedValue, existingValue)) {
        throw new OperationException(StatusCode.WRITE_CONFLICT, "CompareAndSwap expected value mismatch");
      }

      // if newValue is null, just delete.
      if (newValue == null) {
        deleteAll(row, column, writeVersion);
        return;
      }

      // Checks passed, write new value
      performInsert(row, column, writeVersion, Type.Put, newValue);
    } finally {
      this.locks.unlockAndRemove(r);
    }
  }

  @Override
  public boolean compareAndSwapDirty(byte[] row, byte[] column, byte[] expectedValue, byte[] newValue)
    throws OperationException {
    RowLockTable.Row r = new RowLockTable.Row(row);
    this.locks.validLock(r);
    try {
      // Read existing value
      OperationResult<byte[]> readResult = get(row, column, TransactionOracle.DIRTY_READ_POINTER);
      byte[] oldValue = readResult.getValue();

      if ((oldValue == null && expectedValue == null) || Bytes.equals(oldValue, expectedValue)) {
        // if newValue is null, just delete.
        if (newValue == null) {
          KeyValue kv = new KeyValue(row, FAMILY, column, TransactionOracle.DIRTY_WRITE_VERSION, new byte[0]);
          // This deletes only TransactionOracle.DIRTY_WRITE_VERSION
          db.delete(kv.getKey());
        } else {
          performInsert(row, column, TransactionOracle.DIRTY_WRITE_VERSION, Type.Put, newValue);
        }
        return true;
      }

      return false;
    } finally {
      this.locks.unlockAndRemove(r);
    }
  }

  private OperationException createOperationException(Exception e, String where) {
    String msg = "LevelDB exception on " + where + "(error code = " +
      e.getMessage() + ")";
    LOG.error(msg, e);
    return new OperationException(StatusCode.SQL_ERROR, msg, e);
  }

  /**
   * Scanner on top of levelDB dbiterator.
   */
  public class LevelDBScanner implements Scanner {

    private final DBIterator iterator;
    private final byte [] endRow;
    private final ReadPointer readPointer;

    public LevelDBScanner(DBIterator iterator, byte [] startRow, byte[] endRow, ReadPointer readPointer) {
      this.iterator = iterator;
      if (startRow == null) {
        iterator.seekToFirst();
      } else {
        this.iterator.seek(createStartKey(startRow));
      }
      this.endRow = endRow;
      this.readPointer = readPointer;
    }

    private boolean isVisible(KeyValue keyValue) {
      if (readPointer != null && !readPointer.isVisible(keyValue.getTimestamp())) {
        return false;
      } else {
        return true;
      }
    }

    @Override
    public ImmutablePair<byte[], Map<byte[], byte[]>> next() {
      //From the current iterator do one of the following:
      // a) get all columns for current visible row if it is not the endRow
      // b) return null if we have reached endRow
      // c) return null if there are no more entries

      long lastDelete = -1;
      long undeleted = -1;
      byte[] lastRow = new byte[0];
      byte[] lastCol = new byte[0];
      byte[] curCol = new byte[0];

      Map<byte[], byte[]> columnValues = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);

      while (iterator.hasNext()) {

        Map.Entry<byte[], byte[]> entry = iterator.peekNext();
        KeyValue keyValue = createKeyValue(entry.getKey(), entry.getValue());

        if (endRow != null && Bytes.compareTo(keyValue.getRow(), endRow) >= 0) {
          //already reached the end. So break.
          break;
        }

        if (!Bytes.equals(lastRow, keyValue.getRow())) {
          lastDelete = -1;
          undeleted = -1;
          lastCol = new byte[0];
          curCol = new byte[0];
          if (columnValues.size() > 0){
            //If we have reached here. We have read all columns for a single row - since current row is not the same
            // as previous row and we have collected atleast one valid value in the columnValues collection. Break.
            break;
          }
        }

        lastRow = keyValue.getRow();
        iterator.next();

        if (!isVisible(keyValue)) {
          continue;
        }

        if (Bytes.equals(lastCol, keyValue.getQualifier())) {
          continue;
        }

        if (!Bytes.equals(curCol, keyValue.getQualifier())) {
          curCol = keyValue.getQualifier();
          lastDelete = -1;
          undeleted = -1;
        }

        long curVersion = keyValue.getTimestamp();
        Type type = Type.codeToType(keyValue.getType());

        if (type == Type.Delete) {
          lastDelete = curVersion;
          continue;
        }
        if (curVersion == lastDelete) {
          continue;
        }

        if (type == Type.UndeleteColumn) {
          undeleted = curVersion;
          continue;
        }

        if (type == Type.DeleteColumn) {
          if (undeleted == curVersion) {
            continue;
          } else {
            lastCol = keyValue.getQualifier();
            continue;
          }
        }

        if (type == Type.Put) {
          if (curVersion != lastDelete) {
            // If we get here, this version is visible - so add it!
            columnValues.put(keyValue.getQualifier(), keyValue.getValue());
            lastCol = keyValue.getQualifier();
          }
        }
      }
      if (columnValues.size() == 0) {
        return null;
      } else {
        return new ImmutablePair<byte[], Map<byte[], byte[]>>(lastRow, columnValues);

      }
    }

    @Override
    public void close() {
      try {
        iterator.close();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
