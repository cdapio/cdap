package com.continuuity.data.engine.leveldb;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

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

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.Scanner;

/**
 * Implementation of an OVCTable over a HyperSQL table.
 */
public class LevelDBOVCTable
implements OrderedVersionedColumnarTable {

  private static final Logger LOG =
      LoggerFactory.getLogger(LevelDBOVCTable.class);

  private static final String dbFilePrefix = "ldb_";

  private static final byte [] FAMILY = new byte [] { 'f' };
  
  private static final byte [] NULL_VAL = new byte [0];

  private final String basePath;

  @SuppressWarnings("unused")
  private final String tableName;
  
  private final String encodedTableName;

  private DB db;

  LevelDBOVCTable(final String basePath, final String tableName) {
    this.basePath = basePath;
    this.tableName = tableName;
    try {
      this.encodedTableName = URLEncoder.encode(tableName, "ASCII");
    } catch (UnsupportedEncodingException e) {
      LOG.error("Error encoding table name", e);
      throw new RuntimeException(e);
    }
  }

  private String generateDBPath() {
    return basePath + System.getProperty("file.separator") +
        dbFilePrefix + encodedTableName;
  }

  private Options generateDBOptions(boolean createIfMissing,
      boolean errorIfExists) {
    Options options = new Options();
    options.createIfMissing(createIfMissing);
    options.errorIfExists(errorIfExists);
    options.comparator(new KeyValueDBComparator());
    // Disabling optimizations until all tests pass
    // options.blockSize(1024);
    // options.cacheSize(1024*1024*32);
    return options;
  }

  synchronized boolean openTable() throws OperationException {
    try {
      this.db = factory.open(new File(generateDBPath()),
          generateDBOptions(false, false));
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  synchronized void initializeTable() throws OperationException {
    try {
      this.db = factory.open(new File(generateDBPath()),
          generateDBOptions(true, false));
    } catch (IOException e) {
      handleIOException(e, "create");
    }
  }

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

  public static interface ColumnMatcher {
    public boolean includeColumn(byte [] column);
  }

  // LevelDB specific helpers

  private byte [] createStartKey(byte [] row) {
    return new KeyValue(
        row, FAMILY, null, HConstants.LATEST_TIMESTAMP, Type.Maximum)
        .getKey();
  }

  private byte [] createStartKey(byte [] row, byte [] column) {
    return new KeyValue(
        row, FAMILY, column, HConstants.LATEST_TIMESTAMP, Type.Maximum)
        .getKey();
  }

  private byte [] createEndKey(byte [] row) {
    return new KeyValue(
        row, null, null, HConstants.LATEST_TIMESTAMP, Type.Minimum)
        .getKey();
  }

  private byte [] createEndKey(byte [] row, byte [] column) {
    return new KeyValue(
        row, FAMILY, column, 0L, Type.Minimum)
        .getKey();
  }

  private List<KeyValue> readKeyValueRange(byte [] start,
      byte [] end)
  throws DBException, IOException {
    DBIterator iterator = db.iterator();
    try {
      List<KeyValue> kvs = new ArrayList<KeyValue>();
      for (iterator.seek(start); iterator.hasNext(); iterator.next()) {
        byte [] key = iterator.peekNext().getKey();
        byte [] value = iterator.peekNext().getValue();
        // If we reach the end, finish
        if (KeyValue.KEY_COMPARATOR.compare(key, end) >= 0) return kvs;
        kvs.add(createKeyValue(key, value));
      }
      return kvs;
    } finally {
      iterator.close();
    }
  }

  private KeyValue createKeyValue(byte[] key, byte[] value) {
    int len = key.length + value.length + (2 * Bytes.SIZEOF_INT);
    byte [] kvBytes = new byte[len];
    int pos = 0;
    pos = Bytes.putInt(kvBytes, pos, key.length);
    pos = Bytes.putInt(kvBytes, pos, value.length);
    pos = Bytes.putBytes(kvBytes, pos, key, 0, key.length);
    pos = Bytes.putBytes(kvBytes, pos, value, 0, value.length);
    return new KeyValue(kvBytes);
  }

  // Administrative Operations

  @Override
  public synchronized void clear() throws OperationException {
    try {
      db.close();
      factory.destroy(new File(generateDBPath()), new Options());
    } catch (IOException e) {
      handleIOException(e, "clearing");
    }
    initializeTable();
  }

  // Simple Write Operations

  @Override
  public void put(byte[] row, byte[] column, long version, byte[] value)
      throws OperationException {
    performInsert(row, column, version, Type.Put, value);
  }

  @Override
  public void put(byte[] row, byte[][] columns, long version, byte[][] values)
      throws OperationException {
    performInsert(row, columns, version, Type.Put, values);
  }

  // Delete Operations

  @Override
  public void delete(byte[] row, byte[] column, long version)
      throws OperationException {
    performInsert(row, column, version, Type.Delete, NULL_VAL);
  }

  @Override
  public void delete(byte[] row, byte[][] columns, long version)
      throws OperationException {
    performInsert(row, columns, version, Type.Delete,
        generateDeleteVals(columns.length));
  }

  @Override
  public void deleteAll(byte[] row, byte[] column, long version)
      throws OperationException {
    performInsert(row, column, version, Type.DeleteColumn, NULL_VAL);
  }

  @Override
  public void deleteAll(byte[] row, byte[][] columns, long version)
      throws OperationException {
    performInsert(row, columns, version, Type.DeleteColumn,
        generateDeleteVals(columns.length));
  }

  @Override
  public void undeleteAll(byte[] row, byte[] column, long version)
      throws OperationException {
    performInsert(row, column, version, Type.UndeleteColumn, NULL_VAL);
  }

  @Override
  public void undeleteAll(byte[] row, byte[][] columns, long version)
      throws OperationException {
    performInsert(row, columns, version, Type.UndeleteColumn,
        generateDeleteVals(columns.length));
  }

  private byte[][] generateDeleteVals(int length) {
    byte [][] values = new byte[length][];
    for (int i=0;i<values.length;i++) values[i] = NULL_VAL;
    return values;
  }


  // Read Operations

  @Override
  public OperationResult<Map<byte[], byte[]>>
  get(byte[] row, ReadPointer readPointer) throws OperationException {
    try {
      List<KeyValue> kvs = readKeyValueRange(
          createStartKey(row), createEndKey(row));
      return new OperationResult<Map<byte[], byte[]>>(
          filteredLatestColumns(kvs, readPointer, new ColumnMatcher() {
            @Override
            public boolean includeColumn(byte[] column) {
              return true;
            }
          }));
      
    } catch (IOException e) {
      handleIOException(e, "get");
    }
    throw new InternalError("this point should never be reached.");
  }

  @Override
  public OperationResult<byte[]>
  get(byte[] row, byte[] column, ReadPointer readPointer)
      throws OperationException {
    OperationResult<ImmutablePair<byte[], Long>> res =
        getWithVersion(row, column, readPointer);
    if (res.isEmpty())
      return new OperationResult<byte[]>(res.getStatus(), res.getMessage());
    else
      return new OperationResult<byte[]>(res.getValue().getFirst());
  }

  @Override
  public OperationResult<ImmutablePair<byte[], Long>>
  getWithVersion(byte[] row, byte[] column, ReadPointer readPointer)
      throws OperationException {
    try {
      List<KeyValue> kvs = readKeyValueRange(
          createStartKey(row, column), createEndKey(row, column));
      
      ImmutablePair<Long,byte[]> latest = filteredLatest(kvs, readPointer);

      if (latest == null)
        return new OperationResult<ImmutablePair<byte[], Long>>(
            StatusCode.KEY_NOT_FOUND);

      return new OperationResult<ImmutablePair<byte[], Long>>(
          new ImmutablePair<byte[],Long>(
              latest.getSecond(), latest.getFirst()));

    } catch (IOException e) {
      handleIOException(e, "get");
    }
    throw new InternalError("this point should never be reached.");
  }

  @Override
  public OperationResult<Map<byte[], byte[]>>
  get(byte[] row, final byte[] startColumn, final byte[] stopColumn, int limit,
      ReadPointer readPointer) throws OperationException {
    try {
      byte [] startKey = startColumn == null ? createStartKey(row) :
        createStartKey(row, startColumn);
      byte [] endKey = stopColumn == null ? createEndKey(row) :
        createEndKey(row, stopColumn);
      List<KeyValue> kvs = readKeyValueRange(startKey, endKey);
    
      if (kvs == null || kvs.isEmpty()) {
        return new OperationResult<Map<byte[], byte[]>>(
            StatusCode.KEY_NOT_FOUND);
      }
      Map<byte[], byte[]> filtered = filteredLatestColumns(kvs, readPointer,
          limit, new ColumnMatcher() {
              @Override
              public boolean includeColumn(byte[] column) {
                if (startColumn != null &&
                    Bytes.compareTo(column, startColumn) < 0) {
                  return false;
                }
                if (stopColumn != null &&
                    Bytes.compareTo(column, stopColumn) >= 0) {
                  return false;
                }
                return true;
              }
            });
      if (filtered.isEmpty()) {
        return new OperationResult<Map<byte[], byte[]>>(
            StatusCode.COLUMN_NOT_FOUND);
      } else {
        return new OperationResult<Map<byte[], byte[]>>(filtered);
      }
    } catch (IOException e) {
      handleIOException(e, "get");
    }
    throw new InternalError("this point should never be reached.");
  }

  @Override
  public OperationResult<Map<byte[], byte[]>>
  get(byte[] row, byte[][] columns, ReadPointer readPointer)
      throws OperationException {
    try {
      byte [][] orderedColumns = Arrays.copyOf(columns, columns.length);
      Arrays.sort(orderedColumns, new Bytes.ByteArrayComparator());
      byte [] startKey = createStartKey(row, orderedColumns[0]);
      byte [] endKey = createEndKey(row, orderedColumns[columns.length - 1]);
      List<KeyValue> kvs = readKeyValueRange(startKey, endKey);
    
      if (kvs == null || kvs.isEmpty()) {
        return new OperationResult<Map<byte[], byte[]>>(
            StatusCode.KEY_NOT_FOUND);
      }
      final Set<byte[]> columnSet = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
      for (byte [] column : columns) columnSet.add(column);
      Map<byte[], byte[]> filtered =
          filteredLatestColumns(kvs, readPointer, new ColumnMatcher() {
              @Override
              public boolean includeColumn(byte[] column) {
                return columnSet.contains(column);
              }
          });
      if (filtered.isEmpty()) {
        return new OperationResult<Map<byte[], byte[]>>(
            StatusCode.COLUMN_NOT_FOUND);
      } else {
        return new OperationResult<Map<byte[], byte[]>>(filtered);
      }
    } catch (IOException e) {
      handleIOException(e, "get");
    }
    throw new InternalError("this point should never be reached.");
  }

  // Scan Operations

  @Override
  public List<byte[]> getKeys(int limit, int offset,
      ReadPointer readPointer)
      throws OperationException {
    DBIterator iterator = db.iterator();
    try {
      List<KeyValue> kvs = new ArrayList<KeyValue>();
      for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
        byte [] key = iterator.peekNext().getKey();
        byte [] value = iterator.peekNext().getValue();
        kvs.add(createKeyValue(key, value));
      }
      
      List<byte[]> keys = new ArrayList<byte[]>(limit > 1024 ? 1024 : limit);
      int returned = 0;
      int skipped = 0;
      long lastDelete = -1;
      long undeleted = -1;
      byte [] lastRow = new byte[0];
      byte [] curRow = new byte[0];
      byte [] curCol = new byte [0];
      byte [] lastCol = new byte [0];
      for (KeyValue kv : kvs) {
        if (returned >= limit) break;

        // See if we already included this row
        byte [] row = kv.getRow();
        if (Bytes.equals(lastRow, row)) continue;

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
        if (!readPointer.isVisible(curVersion)) continue;

        byte [] column = kv.getQualifier();
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
          if (undeleted == curVersion) continue;
          else {
            // The rest of this column has been deleted, act like we returned it
            lastCol = column;
            continue;
          }
        }
        if (type == Type.Delete) {
          lastDelete = curVersion;
          continue;
        }
        if (curVersion == lastDelete) continue;
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
        handleIOException(e, "closing iterator");
      }
    }
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow, ReadPointer readPointer) {
    throw new UnsupportedOperationException("Scans currently not supported");
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow, byte[][] columns,
      ReadPointer readPointer) {
    throw new UnsupportedOperationException("Scans currently not supported");
  }

  @Override
  public Scanner scan(ReadPointer readPointer) {
    throw new UnsupportedOperationException("Scans currently not supported");
  }

  // Private Helper Methods

  private void performInsert(byte [] row, byte [] column,
      long version,Type type, byte [] value) throws OperationException {
    KeyValue kv = new KeyValue(row, FAMILY, column, version, type, value);
    try {
      WriteOptions options = new WriteOptions();
      // options.sync(true); We can enable fsync() on every write, off for now
      db.put(kv.getKey(), kv.getValue(), options);
    } catch (DBException dbe) {
      handleDBException(dbe, "insert");
    }
  }

  private void performInsert(byte [] row, byte [][] columns,
      long version, Type type, byte [][] values) throws OperationException {
    for (int i=0; i<columns.length; i++) {
      performInsert(row, columns[i], version, type, values[i]);
    }
  }

  /**
   * Result has (version, kvtype, id, value)
   * @throws DBException
   */
  private ImmutablePair<Long, byte[]> filteredLatest(
      List<KeyValue> kvs, ReadPointer readPointer) throws DBException {
    if (kvs == null || kvs.isEmpty()) return null;
    long lastDelete = -1;
    long undeleted = -1;
    for (KeyValue kv : kvs) {
      long curVersion = kv.getTimestamp();
      if (!readPointer.isVisible(curVersion)) continue;
      Type type = Type.codeToType(kv.getType());
      if (type == Type.UndeleteColumn) {
        undeleted = curVersion;
        continue;
      }
      if (type == Type.DeleteColumn) {
        if (undeleted == curVersion) continue;
        else break;
      }
      if (type == Type.Delete) {
        lastDelete = curVersion;
        continue;
      }
      if (curVersion == lastDelete) continue;
      return new ImmutablePair<Long, byte[]>(curVersion, kv.getValue());
    }
    return null;
  }

  /**
   * Result has (column, version, kvtype, id, value)
   * @throws DBException
   */
  private Map<byte[], byte[]> filteredLatestColumns(List<KeyValue> kvs,
      ReadPointer readPointer, int limit, ColumnMatcher columnMatcher)
          throws DBException {

    // negative limit means unlimited results
    if (limit <= 0) limit = Integer.MAX_VALUE;

    Map<byte[],byte[]> map = new TreeMap<byte[],byte[]>(Bytes.BYTES_COMPARATOR);
    if (kvs == null || kvs.isEmpty()) return map;
    byte [] curCol = new byte [0];
    byte [] lastCol = new byte [0];
    long lastDelete = -1;
    long undeleted = -1;
    for (KeyValue kv : kvs) {
      long curVersion = kv.getTimestamp();
      // Check if this entry is visible, skip if not
      if (!readPointer.isVisible(curVersion)) continue;
      byte [] column = kv.getQualifier();
      // Check if this column should be included
      if (!columnMatcher.includeColumn(column)) {
        continue;
      }
      // Check if this column has already been included in result, skip if so
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
        if (undeleted == curVersion) continue;
        else {
          // The rest of this column has been deleted, act like we returned it
          lastCol = column;
          continue;
        }
      }
      if (type == Type.Delete) {
        lastDelete = curVersion;
        continue;
      }
      if (curVersion == lastDelete) continue;
      lastCol = column;
      map.put(column, kv.getValue());

      // break out if limit reached
      if (map.size() >= limit) break;
    }
    return map;
  }

  private Map<byte[], byte[]> filteredLatestColumns(List<KeyValue> kvs,
      ReadPointer readPointer, ColumnMatcher columnMatcher) throws DBException {

    Map<byte[],byte[]> map = new TreeMap<byte[],byte[]>(Bytes.BYTES_COMPARATOR);
    if (kvs == null || kvs.isEmpty()) return map;
    byte [] curCol = new byte [0];
    byte [] lastCol = new byte [0];
    long lastDelete = -1;
    long undeleted = -1;
    for (KeyValue kv : kvs) {
      long curVersion = kv.getTimestamp();
      // Check if this entry is visible, skip if not
      if (!readPointer.isVisible(curVersion)) continue;
      byte [] column = kv.getQualifier();
      // Check if this column is being requested
      if (!columnMatcher.includeColumn(column)) {
        continue;
      }
      // Check if this column has already been included in result, skip if so
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
        if (undeleted == curVersion) continue;
        else {
          // The rest of this column has been deleted, act like we returned it
          lastCol = column;
          continue;
        }
      }
      if (type == Type.Delete) {
        lastDelete = curVersion;
        continue;
      }
      if (curVersion == lastDelete) continue;
      lastCol = column;
      map.put(column, kv.getValue());

    }
    return map;
  }

  // Read-Modify-Write Operations

  @Override
  public synchronized long increment(byte[] row, byte[] column, long amount,
      ReadPointer readPointer, long writeVersion) throws OperationException {
    long newAmount = amount;
    // Read existing value
    OperationResult<byte[]> readResult = get(row, column, readPointer);
    if (!readResult.isEmpty()) {
      newAmount += Bytes.toLong(readResult.getValue());
    }
    // Write new value
    performInsert(row, column, writeVersion, Type.Put,
        Bytes.toBytes(newAmount));
    return newAmount;
  }

  @Override
  public synchronized Map<byte[], Long> increment(byte[] row, byte[][] columns,
      long[] amounts, ReadPointer readPointer, long writeVersion)
      throws OperationException {
    Map<byte[],Long> ret = new TreeMap<byte[],Long>(Bytes.BYTES_COMPARATOR);
    for (int i=0; i<columns.length; i++) {
      ret.put(columns[i], increment(row, columns[i], amounts[i],
          readPointer, writeVersion));
    }
    return ret;
  }

  @Override
  public synchronized void compareAndSwap(byte[] row, byte[] column,
      byte[] expectedValue, byte[] newValue, ReadPointer readPointer,
      long writeVersion) throws OperationException {

    // Read existing value
    OperationResult<byte[]> readResult = get(row, column, readPointer);
    byte [] existingValue = readResult.getValue();
    
    // Handle cases regarding non-existent values
    if (existingValue == null && expectedValue != null)
      throw new OperationException(StatusCode.WRITE_CONFLICT,
          "CompareAndSwap expected value mismatch");
    if (existingValue != null && expectedValue == null)
      throw new OperationException(StatusCode.WRITE_CONFLICT,
          "CompareAndSwap expected value mismatch");
    
    // if nothing existed, write data
    if (expectedValue == null) {
      performInsert(row, column, writeVersion, Type.Put, newValue);
      return;
    }

    // check if expected == existing, fail if not
    if (!Bytes.equals(expectedValue, existingValue))
      throw new OperationException(StatusCode.WRITE_CONFLICT,
          "CompareAndSwap expected value mismatch");

    // if newValue is null, just delete.
    if (newValue == null) {
      deleteAll(row, column, writeVersion);
      return;
    }

    // Checks passed, write new value
    performInsert(row, column, writeVersion, Type.Put, newValue);
  }
  
  private void handleIOException(IOException e, String where)
      throws OperationException {
    String msg = "LevelDB exception on " + where + "(error code = " +
        e.getMessage() + ")";
    LOG.error(msg, e);
    throw new OperationException(StatusCode.SQL_ERROR, msg, e);
  }
  
  private void handleDBException(DBException e, String where)
      throws OperationException {
    String msg = "LevelDB exception on " + where + "(error code = " +
        e.getMessage() + ")";
    LOG.error(msg, e);
    throw new OperationException(StatusCode.SQL_ERROR, msg, e);
  }
}
