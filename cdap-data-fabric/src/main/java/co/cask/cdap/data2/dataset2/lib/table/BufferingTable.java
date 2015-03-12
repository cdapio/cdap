/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.metrics.MeteredDataset;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Result;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.TableSplit;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import javax.annotation.Nullable;

/**
 * An abstract {@link TransactionAware} implementation of {@link co.cask.cdap.api.dataset.table.Table} which
 * keeps data in memory buffer until transaction commits.
 * <p>
 * Subclasses should implement methods which deal with persistent store. This implementation merges data from persistent
 * store and in-memory buffer for read/write operations.
 * NOTE: this implementation does not allow storing null as a value of a column
 * NOTE: data fetched from persisted store should never have nulls in column values: this class doesn't check that and
 *       they could be exposed to user as nulls. At the same time since null value is not allowed by this implementation
 *       this could lead to un-expected results
 * <p>
 * This implementation assumes that the table has name and conflicts are resolved on row level.
 * <p>
 * NOTE: this implementation doesn't cache any data in-memory besides changes. I.e. if you do get of same data that is
 *       not in in-memory buffer twice, two times it will try to fetch it from persistent store.
 *       Given the snapshot isolation tx model, this can be improved in future implementations.
 * <p>
 * NOTE: current implementation persists changes only at the end of transaction. Beware of OOME. There should be better
 *       implementation for MapReduce case (YMMV though, for counters/aggregations this implementation looks sweet)
 * <p>
 * NOTE: Using {@link #get(byte[], byte[], byte[], int)} is generally always not efficient since it always hits the
 *       persisted store even if all needed data is in-memory buffer. See more info at method javadoc
 */
// todo: copying passed params to write methods may be done more efficiently: no need to copy when no changes are made
public abstract class BufferingTable extends AbstractTable implements MeteredDataset {

  private static final Logger LOG = LoggerFactory.getLogger(BufferingTable.class);

  protected static final byte[] DELETE_MARKER = new byte[0];

  // name of the table
  private final String name;
  // conflict detection level
  private final ConflictDetection conflictLevel;
  // name length + name of the table: handy to have one cached
  private final byte[] nameAsTxChangePrefix;
  // Whether read-less increments should be used when increment() is called
  private final boolean enableReadlessIncrements;

  // In-memory buffer that keeps not yet persisted data. It is row->(column->value) map. Value can be null which means
  // that the corresponded column was removed.
  private NavigableMap<byte[], NavigableMap<byte[], Update>> buff;

  // Keeps track of what was persisted so far
  private NavigableMap<byte[], NavigableMap<byte[], Update>> toUndo;

  // Report data ops metrics to
  private MetricsCollector metricsCollector;

  /**
   * Creates an instance of {@link BufferingTable}.
   * @param name table name
   */
  public BufferingTable(String name) {
    this(name, ConflictDetection.ROW);
  }

  /**
   * Creates an instance of {@link BufferingTable}.
   * @param name table name
   */
  public BufferingTable(String name, ConflictDetection level) {
    this(name, level, false);
  }

  /**
   * Creates an instance of {@link BufferingTable}.
   * @param name table name
   */
  public BufferingTable(String name, ConflictDetection level, boolean enableReadlessIncrements) {
    // for optimization purposes we don't allow table name of length greater than Byte.MAX_VALUE
    Preconditions.checkArgument(name.length() < Byte.MAX_VALUE,
                                "Too big table name: " + name + ", exceeds " + Byte.MAX_VALUE);
    this.name = name;
    this.conflictLevel = level;
    this.enableReadlessIncrements = enableReadlessIncrements;
    // TODO: having central dataset management service will allow us to use table ids instead of names, which will
    //       reduce changeset size transferred to/from server
    // we want it to be of format length+value to avoid conflicts like table="ab", row="cd" vs table="abc", row="d"
    // Default uses the above scheme. Subclasses can change it by overriding the #getNameAsTxChangePrefix method
    this.nameAsTxChangePrefix = Bytes.add(new byte[]{(byte) name.length()}, Bytes.toBytes(name));
    this.buff = new ConcurrentSkipListMap<byte[], NavigableMap<byte[], Update>>(Bytes.BYTES_COMPARATOR);
  }

  /**
   * @return name of this table
   */
  public String getTableName() {
    return name;
  }

  @Override
  public String getTransactionAwareName() {
    return getClass().getSimpleName() + "(table = " + name + ")";
  }

  /**
   * Generates a byte array to be used as the transaction change prefix.
   * Allows implementations to override it so the change prefix more closely represents the underlying storage.
   *
   * @return transaction change prefix
   */
  public byte[] getNameAsTxChangePrefix() {
    return this.nameAsTxChangePrefix;
  }

  /**
   * Persists in-memory buffer. After this method returns we assume that data can be visible to other table clients
   * (of course other clients may choose still not to see it based on transaction isolation logic).
   * @param buff in-memory buffer to persist. Map is described as row->(column->value). Map can contain null values
   *             which means that the corresponded column was deleted
   * @throws Exception
   */
  protected abstract void persist(NavigableMap<byte[], NavigableMap<byte[], Update>> buff)
    throws Exception;

  /**
   * Undos previously persisted changes. After this method returns we assume that data can be visible to other table
   * clients (of course other clients may choose still not to see it based on transaction isolation logic).
   * @param persisted previously persisted changes. Map is described as row->(column->value). Map can contain null
   *                  values which means that the corresponded column was deleted
   * @throws Exception
   */
  protected abstract void undo(NavigableMap<byte[], NavigableMap<byte[], Update>> persisted)
    throws Exception;

  /**
   * Fetches column->value pairs for set of columns from persistent store.
   * NOTE: persisted store can also be in-memory, it is called "persisted" to distinguish from in-memory buffer.
   * @param row row key defines the row to fetch columns from
   * @param columns set of columns to fetch, can be null which means fetch everything
   * @return map of column->value pairs, never null.
   * @throws Exception
   */
  protected abstract NavigableMap<byte[], byte[]> getPersisted(byte[] row, @Nullable byte[][] columns)
    throws Exception;

  /**
   * Fetches column->value pairs for range of columns from persistent store.
   * NOTE: persisted store can also be in-memory, it is called "persisted" to distinguish from in-memory buffer.
   * NOTE: Using this method is generally always not efficient since it always hits the
   *       persisted store even if all needed data is in-memory buffer. Since columns set is not strictly defined the
   *       implementation always looks up for more columns in persistent store.
   * @param row row key defines the row to fetch columns from
   * @param startColumn first column in a range, inclusive
   * @param stopColumn last column in a range, exclusive
   * @param limit max number of columns to fetch
   * @return map of column->value pairs, never null.
   * @throws Exception
   */
  protected abstract NavigableMap<byte[], byte[]> getPersisted(byte[] row,
                                                               byte[] startColumn, byte[] stopColumn,
                                                               int limit)
    throws Exception;

  /**
   * Scans range of rows from persistent store.
   * NOTE: persisted store can also be in-memory, it is called "persisted" to distinguish from in-memory buffer.
   * @param startRow key of the first row in a range, inclusive
   * @param stopRow key of the last row in a range, exclusive
   * @return instance of {@link Scanner}, never null
   * @throws Exception
   */
  protected abstract  Scanner scanPersisted(byte[] startRow, byte[] stopRow) throws Exception;

  @Override
  public void setMetricsCollector(MetricsCollector metricsCollector) {
    this.metricsCollector = metricsCollector;
  }

  @Override
  public void close() {
    // releasing resources
    buff = null;
    toUndo = null;
  }

  @Override
  public void startTx(Transaction tx) {
    if (buff == null) {
      String msg = "Attempted to use closed dataset " + getTransactionAwareName();
      LOG.error(msg);
      throw new IllegalStateException(msg);
    }
    // starting with fresh buffer when tx starts
    buff.clear();
    toUndo = null;
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    switch (conflictLevel) {
      case NONE:
        return Collections.emptyList();
      case ROW:
        return getRowChanges();
      case COLUMN:
        return getColumnChanges();
      default:
        throw new RuntimeException("Unknown conflict detection level: " + conflictLevel);
    }
  }

  private Collection<byte[]> getRowChanges() {
    // we resolve conflicts on row level of individual table
    List<byte[]> changes = new ArrayList<byte[]>(buff.size());
    for (byte[] changedRow : buff.keySet()) {
      changes.add(Bytes.add(getNameAsTxChangePrefix(), changedRow));
    }
    return changes;
  }

  private Collection<byte[]> getColumnChanges() {
    // we resolve conflicts on row level of individual table
    List<byte[]> changes = new ArrayList<byte[]>(buff.size());
    for (Map.Entry<byte[], NavigableMap<byte[], Update>> rowChange : buff.entrySet()) {
      if (rowChange.getValue() == null) {
        // NOTE: as of now we cannot detect conflict between delete whole row and row's column value change.
        //       this is not a big problem as of now, as row deletion is now act as deletion of every column, but this
        //       will change in future, so we will have to address the issue.
        continue;
      }

      // using length + value format to prevent conflicts like row="ab", column="cd" vs row="abc", column="d"
      byte[] rowTxChange = Bytes.add(Bytes.toBytes(rowChange.getKey().length), rowChange.getKey());

      for (byte[] column : rowChange.getValue().keySet()) {
        changes.add(Bytes.add(getNameAsTxChangePrefix(), rowTxChange, column));
      }
    }
    return changes;
  }

  @Override
  public boolean commitTx() throws Exception {
    if (!buff.isEmpty()) {
      // We first assume that all data will be persisted. So that if exception happen during persist we try to
      // rollback everything we had in in-memory buffer.
      toUndo = buff;
      // clearing up in-memory buffer by initializing new map.
      // NOTE: we want to init map here so that if no changes are made we re-use same instance of the map in next tx
      // NOTE: we could cache two maps and swap them to avoid creation of map instances, but code would be ugly
      buff = new ConcurrentSkipListMap<byte[], NavigableMap<byte[], Update>>(Bytes.BYTES_COMPARATOR);
      // TODO: tracking of persisted items can be optimized by returning a pair {succeededOrNot, persisted} which
      //       tells if persisting succeeded and what was persisted (i.e. what we will have to undo in case of rollback)
      persist(toUndo);
    }
    return true;
  }

  @Override
  public void postTxCommit() {
    // don't need buffer anymore: tx has been committed
    buff.clear();
    toUndo = null;
  }

  @Override
  public boolean rollbackTx() throws Exception {
    buff.clear();
    if (toUndo != null) {
      undo(toUndo);
      toUndo = null;
    }
    return true;
  }

  /**
   * NOTE: Depending on the use-case, calling this method may be much less
   *       efficient than calling same method with columns as parameters because it may always require round trip to
   *       persistent store
   */
  @Override
  public Row get(byte[] row) {
    reportRead(1);
    try {
      return new Result(row, getRowMap(row));
    } catch (Exception e) {
      LOG.debug("get failed for table: " + getTransactionAwareName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("get failed", e);
    }
  }

  @Override
  public Row get(byte[] row, byte[][] columns) {
    reportRead(1);
    try {
      return new Result(row, getRowMap(row, columns));
    } catch (Exception e) {
      LOG.debug("get failed for table: " + getTransactionAwareName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("get failed", e);
    }
  }

  @Override
  public Row get(byte[] row, byte[] startColumn, byte[] stopColumn, int limit) {
    reportRead(1);
    // checking if the row was deleted inside this tx
    NavigableMap<byte[], Update> buffCols = buff.get(row);
    boolean rowDeleted = buffCols == null && buff.containsKey(row);
    // ANDREAS: can this ever happen?
    if (rowDeleted) {
      return new Result(row, Collections.<byte[], byte[]>emptyMap());
    }

    // NOTE: since we cannot tell the exact column set, we always have to go to persisted store.
    //       potential improvement: do not fetch columns available in in-mem buffer (we know them at this point)
    try {
      Map<byte[], byte[]> persistedCols = getPersisted(row, startColumn, stopColumn, limit);

      // adding server cols, and then overriding with buffered values
      NavigableMap<byte[], byte[]> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      if (persistedCols != null) {
        result.putAll(persistedCols);
      }

      if (buffCols != null) {
        buffCols = getRange(buffCols, startColumn, stopColumn, limit);
        // null valued columns in in-memory buffer are deletes, so we need to delete them from the result list
        mergeToPersisted(result, buffCols, null);
      }

      // applying limit
      return new Result(row, head(result, limit));
    } catch (Exception e) {
      LOG.debug("get failed for table: " + getTransactionAwareName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("get failed", e);
    }
  }

  /**
   * NOTE: if value is null corresponded column is deleted. It will not be in result set when reading.
   *
   * Also see {@link co.cask.cdap.api.dataset.table.Table#put(byte[], byte[][], byte[][])}.
   */
  @Override
  public void put(byte[] row, byte[][] columns, byte[][] values) {
    NavigableMap<byte[], Update> colVals = buff.get(row);
    boolean newRow = false;
    if (colVals == null) {
      colVals = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      newRow = true;
    }
    for (int i = 0; i < columns.length; i++) {
      // NOTE: we copy passed column's and value's byte arrays to protect buffer against possible changes of these
      // arrays on client
      Preconditions.checkArgument(values[i] == null || values[i].length > 0,
                                  "Write of an empty value is not supported");
      colVals.put(copy(columns[i]), new PutValue(copy(values[i])));
    }
    if (newRow) {
      // NOTE: we copy passed row's byte arrays to protect buffer against possible changes of this array on client
      buff.put(copy(row), colVals);
    }
    // report metrics _after_ write was performed
    reportWrite(1, getSize(row) + getSize(columns) + getSize(values));
  }

  /**
   * NOTE: Depending on the use-case, calling this method may be much less
   *       efficient than calling same method with columns as parameters because it may always require round trip to
   *       persistent store
   */
  @Override
  public void delete(byte[] row) {
    // "0" because we don't know what gets deleted
    reportWrite(1, 0);
    // this is going to be expensive, but the only we can do as delete implementation act on per-column level
    try {
      Map<byte[], byte[]> rowMap = getRowMap(row);
      delete(row, rowMap.keySet().toArray(new byte[rowMap.keySet().size()][]));
    } catch (Exception e) {
      LOG.debug("delete failed for table: " + getTransactionAwareName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("delete failed", e);
    }
  }

  @Override
  public void delete(byte[] row, byte[][] columns) {
    if (columns == null) {
      delete(row);
      return;
    }

    // Do not delete anything when columns list is empty. Return-fast shortcut
    if (columns.length == 0) {
      return;
    }

    // "0" because we don't know what gets deleted
    reportWrite(1, 0);
    // same as writing null for every column
    // ANDREAS: shouldn't this be DELETE_MARKER?
    put(row, columns, new byte[columns.length][]);
  }

  @Override
  public Row incrementAndGet(byte[] row, byte[][] columns, long[] amounts) {
    reportRead(1);
    reportWrite(1, getSize(row) + getSize(columns) + getSize(amounts));
    // Logic:
    // * fetching current values
    // * updating values
    // * updating in-memory store
    // * returning updated values as result
    // NOTE: there is more efficient way to do it, but for now we want more simple implementation, not over-optimizing
    Map<byte[], byte[]> rowMap;
    try {
      rowMap = getRowMap(row, columns);
    } catch (Exception e) {
      LOG.debug("incrementAndGet failed for table: " + getTransactionAwareName() +
          ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("incrementAndGet failed", e);
    }

    byte[][] updatedValues = new byte[columns.length][];

    NavigableMap<byte[], byte[]> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (int i = 0; i < columns.length; i++) {
      byte[] column = columns[i];
      byte[] val = rowMap.get(column);
      // converting to long
      long longVal;
      if (val == null) {
        longVal = 0L;
      } else {
        if (val.length != Bytes.SIZEOF_LONG) {
          throw new NumberFormatException("Attempted to increment a value that is not convertible to long," +
                                            " row: " + Bytes.toStringBinary(row) +
                                            " column: " + Bytes.toStringBinary(column));
        }
        longVal = Bytes.toLong(val);
      }
      longVal += amounts[i];
      updatedValues[i] = Bytes.toBytes(longVal);
      result.put(column, updatedValues[i]);
    }

    put(row, columns, updatedValues);

    return new Result(row, result);
  }

  @Override
  public void increment(byte[] row, byte[][] columns, long[] amounts) {
    if (enableReadlessIncrements) {
      reportWrite(1, getSize(row) + getSize(columns) + getSize(amounts));
      NavigableMap<byte[], Update> colVals = buff.get(row);
      if (colVals == null) {
        colVals = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        buff.put(row, colVals);
      }
      for (int i = 0; i < columns.length; i++) {
        colVals.put(columns[i], Updates.mergeUpdates(colVals.get(columns[i]), new IncrementValue(amounts[i])));
      }
    } else {
      incrementAndGet(row, columns, amounts);
    }
  }

  @Override
  public boolean compareAndSwap(byte[] row, byte[] column, byte[] expectedValue, byte[] newValue) {
    Preconditions.checkArgument(newValue == null || newValue.length > 0, "Write of an empty value is not supported");
    reportRead(1);
    reportWrite(1, getSize(row) + getSize(column) + getSize(newValue));
    // NOTE: there is more efficient way to do it, but for now we want more simple implementation, not over-optimizing
    byte[][] columns = new byte[][]{column};
    try {
      byte[] currentValue = getRowMap(row, columns).get(column);
      if (Arrays.equals(expectedValue, currentValue)) {
        put(row, columns, new byte[][]{newValue});
        return true;
      }
    } catch (Exception e) {
      LOG.debug("compareAndSwap failed for table: " + getTransactionAwareName() +
          ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("compareAndSwap failed", e);
    }

    return false;
  }

  /**
   * Fallback implementation of getSplits, {@link SplitsUtil#primitiveGetSplits(int, byte[], byte[])}.
   * Ideally should be overridden by subclasses.
   *
   * @param numSplits Desired number of splits. If greater than zero, at most this many splits will be returned.
   *                  If less or equal to zero, any number of splits can be returned.
   * @param start If non-null, the returned splits will only cover keys that are greater or equal.
   * @param stop If non-null, the returned splits will only cover keys that are less.
   * @return list of {@link Split}
   */
  @Override
  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) {
    List<KeyRange> keyRanges = SplitsUtil.primitiveGetSplits(numSplits, start, stop);
    return Lists.transform(keyRanges, new Function<KeyRange, Split>() {
      @Nullable
      @Override
      public Split apply(@Nullable KeyRange input) {
        return new TableSplit(input == null ? null : input.getStart(),
                                           input == null ? null : input.getStop());
      }
    });
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow) {
    NavigableMap<byte[], NavigableMap<byte[], Update>> bufferMap;
    if (startRow == null && stopRow == null) {
      bufferMap = buff;
    } else if (startRow == null) {
      bufferMap = buff.headMap(stopRow, false);
    } else if (stopRow == null) {
      bufferMap = buff.tailMap(startRow, true);
    } else {
      bufferMap = buff.subMap(startRow, true, stopRow, false);
    }
    try {
      return new BufferingScanner(bufferMap, scanPersisted(startRow, stopRow));
    } catch (Exception e) {
      LOG.debug("scan failed for table: " + getTransactionAwareName() +
          ", start row: " + (startRow != null ? Bytes.toStringBinary(startRow) : "NULL") +
          ", stop row: "  + (stopRow != null ? Bytes.toStringBinary(stopRow) : "NULL"), e);
      throw new DataSetException("compareAndSwap failed", e);
    }
  }

  private Map<byte[], byte[]> getRowMap(byte[] row) throws Exception {
    NavigableMap<byte[], byte[]> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    // checking if the row was deleted inside this tx
    NavigableMap<byte[], Update> buffCols = buff.get(row);
    boolean rowDeleted = buffCols == null && buff.containsKey(row);
    if (rowDeleted) {
      return Collections.emptyMap();
    }

    Map<byte[], byte[]> persisted = getPersisted(row, null);


    result.putAll(persisted);
    if (buffCols != null) {
      // buffered should override those returned from persistent store
      mergeToPersisted(result, buffCols, null);
    }

    return unwrapDeletes(result);
  }

  private Map<byte[], byte[]> getRowMap(byte[] row, byte[][] columns) throws Exception {
    NavigableMap<byte[], byte[]> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    // checking if the row was deleted inside this tx
    NavigableMap<byte[], Update> buffCols = buff.get(row);
    boolean rowDeleted = buffCols == null && buff.containsKey(row);
    if (rowDeleted) {
      return Collections.emptyMap();
    }

    // if nothing locally, return all from server
    if (buffCols == null) {
      return getPersisted(row, columns);
    }

    // otherwise try to fetch data from in-memory buffer. If not all present - fetch leftover from persisted
    List<byte[]> colsToFetchFromPersisted = Lists.newArrayList();
    // try to fetch from local buffer first and then from server if it is not in buffer
    for (byte[] column : columns) {
      if (!buffCols.containsKey(column)) {
        colsToFetchFromPersisted.add(column);
        continue;
      }

      Update val = buffCols.get(column);
      // buffered increments will need to the applied on top of the persisted values
      if (val instanceof IncrementValue) {
        colsToFetchFromPersisted.add(column);
      }
    }

    // fetching from server those that were not found in in-mem buffer
    if (colsToFetchFromPersisted.size() > 0) {
      Map<byte[], byte[]> persistedCols =
        getPersisted(row, colsToFetchFromPersisted.toArray(new byte[colsToFetchFromPersisted.size()][]));
      if (persistedCols != null) {
        result.putAll(persistedCols);
      }
    }

    // overlay buffered values on persisted, applying increments where necessary
    mergeToPersisted(result, buffCols, columns);

    return unwrapDeletes(result);
  }

  /**
   * Applies the buffered updates on top of the map of persisted values.  The persisted map is modified in place
   * with the updated values.
   * @param persisted The map to modify with the buffered values.
   * @param buffered The buffered values to overlay on the persisted map.
   */
  private static void mergeToPersisted(Map<byte[], byte[]> persisted, Map<byte[], Update> buffered, byte[][] columns) {
    List<byte[]> columnKeys;
    if (columns != null) {
      columnKeys = Arrays.asList(columns);
    } else {
      // NOTE: we want to copy key's byte array because it may be leaked to table's client and we don't want client
      //       to affect the buffer by changing it in place
      columnKeys = Lists.newArrayListWithExpectedSize(buffered.size());
      for (byte[] key : buffered.keySet()) {
        columnKeys.add(copy(key));
      }
    }
    // overlay buffered values on persisted, applying increments where necessary
    for (byte[] key : columnKeys) {
      Update val = buffered.get(key);
      if (val == null) {
        if (buffered.containsKey(key)) {
          persisted.remove(key);
        }
      } else if (val instanceof IncrementValue) {
        long persistedValue = 0L;
        byte[] persistedBytes = persisted.get(key);
        if (persistedBytes != null) {
          persistedValue = Bytes.toLong(persistedBytes);
        }
        long newValue = persistedValue + ((IncrementValue) val).getValue();
        persisted.put(key, Bytes.toBytes(newValue));
      } else if (val instanceof PutValue) {
        // overwrite the current
        // NOTE: we want to copy value's byte array because it may be leaked to table's client and we don't want client
        // to affect the buffer by changing it in place
        persisted.put(key, copy(((PutValue) val).getValue()));
      }
      // unknown type?!
    }
  }

  // utilities useful for underlying implementations

  protected static <T> NavigableMap<byte[], T> getRange(NavigableMap<byte[], T> rowMap,
                                                         byte[] startColumn, byte[] stopColumn,
                                                         int limit) {
    NavigableMap<byte[], T> result;
    if (startColumn == null && stopColumn == null) {
      result = rowMap;
    } else if (startColumn == null) {
      result = rowMap.headMap(stopColumn, false);
    } else if (stopColumn == null) {
      result = rowMap.tailMap(startColumn, true);
    } else {
      result = rowMap.subMap(startColumn, true, stopColumn, false);
    }
    return head(result, limit);
  }

  protected static <T> NavigableMap<byte[], T> head(NavigableMap<byte[], T> map, int count) {
    if (count > 0 && map.size() > count) {
      // todo: is there better way to do it?
      byte [] lastToInclude = null;
      int i = 0;
      for (Map.Entry<byte[], T> entry : map.entrySet()) {
        lastToInclude = entry.getKey();
        if (++i >= count) {
          break;
        }
      }
      map = map.headMap(lastToInclude, true);
    }

    return map;
  }

  protected static byte[] wrapDeleteIfNeeded(byte[] value) {
    return value == null ? DELETE_MARKER : value;
  }

  protected static byte[] unwrapDeleteIfNeeded(byte[] value) {
    return Arrays.equals(DELETE_MARKER, value) ? null : value;
  }

  // todo: it is in-efficient to copy maps a lot, consider merging with getLatest methods
  protected static NavigableMap<byte[], NavigableMap<byte[], byte[]>> unwrapDeletesForRows(
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> rows) {

    NavigableMap<byte[], NavigableMap<byte[], byte[]>> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> row : rows.entrySet()) {
      NavigableMap<byte[], byte[]> rowMap = unwrapDeletes(row.getValue());
      if (rowMap.size() > 0) {
        result.put(row.getKey(), rowMap);
      }
    }

    return result;
  }

  // todo: it is in-efficient to copy maps a lot, consider merging with getLatest methods
  protected static NavigableMap<byte[], byte[]> unwrapDeletes(NavigableMap<byte[], byte[]> rowMap) {
    if (rowMap == null || rowMap.isEmpty()) {
      return EMPTY_ROW_MAP;
    }
    NavigableMap<byte[], byte[]> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], byte[]> keyVal : rowMap.entrySet()) {
      byte[] val = unwrapDeleteIfNeeded(keyVal.getValue());
      if (val != null) {
        result.put(keyVal.getKey(), val);
      }
    }
    return result;
  }

  private void reportWrite(int numOps, int dataSize) {
    if (metricsCollector != null) {
      metricsCollector.recordWrite(numOps, dataSize);
    }
  }

  private void reportRead(int numOps) {
    if (metricsCollector != null) {
      // todo: report amount of data being read
      metricsCollector.recordRead(numOps, 0);
    }
  }

  private int getSize(long[] values) {
    return Bytes.SIZEOF_LONG * values.length;
  }

  private static int getSize(byte[][] data) {
    int size = 0;
    for (byte[] item : data) {
      size += getSize(item);
    }

    return size;
  }

  private static int getSize(byte[] item) {
    return item == null ? 0 : item.length;
  }

  private static byte[] copy(byte[] bytes) {
    return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
  }

  /**
   * Scanner implementation that overlays buffered data on top of already persisted data.
   */
  private class BufferingScanner implements Scanner {
    private final NavigableMap<byte[], NavigableMap<byte[], Update>> buffer;
    private final Scanner persistedScanner;
    private final Iterator<byte[]> keyIter;
    private byte[] currentKey;
    private Row currentRow;

    private BufferingScanner(NavigableMap<byte[], NavigableMap<byte[], Update>> buffer, Scanner persistedScanner) {
      this.buffer = buffer;
      this.keyIter = this.buffer.keySet().iterator();
      if (this.keyIter.hasNext()) {
        currentKey = keyIter.next();
      }
      this.persistedScanner = persistedScanner;
      this.currentRow = this.persistedScanner.next();
    }

    @Nullable
    @Override
    public Row next() {
      if (currentKey == null && currentRow == null) {
        // out of rows
        return null;
      }
      int order;
      if (currentKey == null) {
        // exhausted buffer is the same as persisted scan row coming first
        order = 1;
      } else if (currentRow == null) {
        // exhausted persisted scanner is the same as buffer row coming first
        order = -1;
      } else {
        order = Bytes.compareTo(currentKey, currentRow.getRow());
      }
      Row result = null;
      if (order > 0) {
        // persisted row comes first or buffer is empty
        result = currentRow;
        currentRow = persistedScanner.next();
      } else if (order < 0) {
        // buffer row comes first or persisted scanner is empty
        Map<byte[], byte[]> persistedRow = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        mergeToPersisted(persistedRow, buffer.get(currentKey), null);
        result = new Result(copy(currentKey), persistedRow);

        currentKey = keyIter.hasNext() ? keyIter.next() : null;
      } else {
        // if currentKey and currentRow are equal, merge and advance both
        Map<byte[], byte[]> persisted = currentRow.getColumns();
        mergeToPersisted(persisted, buffer.get(currentKey), null);
        result = new Result(currentRow.getRow(), persisted);

        currentRow = persistedScanner.next();
        currentKey = keyIter.hasNext() ? keyIter.next() : null;
      }
      return result;
    }

    @Override
    public void close() {
      this.persistedScanner.close();
    }
  }
}
