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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.table.Delete;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Result;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.annotation.Nullable;

/**
 * Implements a table that creates and maintains indexes on values stored within a configured set of column names.
 *
 * <p>
 * This dataset uses two tables:
 * <ul>
 *   <li>the actual data table, which stores the raw, unmodified rows which are written; and</li>
 *   <li>an index table, with rows keyed by the indexed column and value (plus data row key for uniqueness),
 *   which contains a reference to the row key in the data table matching the indexed value.</li>
 * </ul>
 * </p>
 *
 * <p>The indexed values need not be unique.  When reading the data back by index value, a {@link Scanner} will be
 * returned, allowing the client to iterate through all matching rows. Exact matches as well as range lookups on
 * indexed values are supported.
 * </p>
 *
 * <p>Index entries are created by storing additional rows in a second table.  These index rows are keyed by
 * column name, column value, and original row key, each field separated by a single null byte delimiter.  It is not
 * recommended that the name of an index column contains the null byte (this can cause a degradation in performance
 * of index reads).
 * </p>
 *
 * <p>The columns to index can be configured in the {@link co.cask.cdap.api.dataset.DatasetProperties} used
 * when the dataset instance in created.  Multiple column names should be listed as a comma-separated string
 * (with no spaces):
 *
 * <p>
 * <pre><code>public class MyApp extends AbstractApplication {
 *   public void configure() {
 *     setName("MyApp");
 *     ...
 *     createDataset("indexedData", IndexedTable.class,
 *                   DatasetProperties.builder().add(
 *                       IndexedTableDefinition.INDEX_COLUMNS_CONF_KEY, "col1,col2").build());
 *     ...
 *   }
 * }</code></pre>
 * </p>
 * 
 * <p>
 * Note that this means that the column names which should be indexed cannot contain the comma character,
 * as it would break parsing of the configuration property.
 * </p>
 *
 * @see co.cask.cdap.api.dataset.lib.IndexedTableDefinition#INDEX_COLUMNS_CONF_KEY
 */
public class IndexedTable extends AbstractDataset implements Table {
  private static final Logger LOG = LoggerFactory.getLogger(IndexedTable.class);

  /**
   * Column key used to store the existence of a row in the secondary index.
   */
  private static final byte[] IDX_COL = {'r'};
  private static final byte DELIMITER_BYTE = 0;
  private static final byte[] KEY_DELIMITER = new byte[] { DELIMITER_BYTE };

  private final boolean hasColumnWithDelimiter;
  // the two underlying tables
  private Table table, index;
  // the secondary index column
  private SortedSet<byte[]> indexedColumns;


  /**
   * Configuration time constructor.
   * 
   * @param name the name of the table
   * @param table table to use as the table
   * @param index table to use as the index
   * @param columnsToIndex the names of the data columns to index
   */
  public IndexedTable(String name, Table table, Table index, byte[][] columnsToIndex) {
    super(name, table, index);
    this.table = table;
    this.index = index;
    this.indexedColumns = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    this.hasColumnWithDelimiter = hasDelimiterByte(columnsToIndex);
    Collections.addAll(this.indexedColumns, columnsToIndex);
  }

  /**
   * Checks if a set of columns contains the DELIMITER_BYTE.
   * This is needed because only when a column has a null byte in it do we need to check for false positive in the scan
   * on the index by checking against the column's value in the data table.
   * For instance, a false positive can arise with an index on the following two rows:
   * <ul>
   *  <li>Row #1: column name: a\0b, column value: c</li>
   *  <li>Row #2: column name: a, column value: b\0c</li>
   * </ul>
   * Because of how the rows are constructed in the index table, both will be prefixed (and therefore looked up by)
   * the row key prefix a\0b\0c.
   *
   * @param columns the set of columns being checked
   * @return true if any of the columns contain the DELIMITER_BYTE
   */
  private boolean hasDelimiterByte(byte[][] columns) {
    for (byte[] column : columns) {
      for (byte b : column) {
        if (b == DELIMITER_BYTE) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Read a row by row key from the data table.
   *
   * @param get the read operation, as if it were on a non-indexed table
   * @return the result of the read on the underlying primary table
   */
  @Override
  public Row get(Get get) {
    return table.get(get);
  }

  @Override
  public Row get(byte[] row) {
    return table.get(row);
  }

  @Override
  public byte[] get(byte[] row, byte[] column) {
    return table.get(row, column);
  }

  @Override
  public Row get(byte[] row, byte[][] columns) {
    return table.get(row, columns);
  }

  @Override
  public Row get(byte[] row, byte[] startColumn, byte[] stopColumn, int limit) {
    return table.get(row, startColumn, stopColumn, limit);
  }

  @Override
  public List<Row> get(List<Get> gets) {
    return table.get(gets);
  }

  /**
   * Reads table rows by the given secondary index key.  If no rows are indexed by the given key, then a
   * {@link co.cask.cdap.api.dataset.table.Scanner} with no results will be returned.
   * 
   * @return a Scanner returning rows from the data table, whose stored value for the given column matches the
   * given value.
   * @throws java.lang.IllegalArgumentException if the given column is not configured for indexing.
   */
  public Scanner readByIndex(byte[] column, byte[] value) {
    assertIndexedColumn(column);
    byte[] rowKeyPrefix = Bytes.concat(column, KEY_DELIMITER, value, KEY_DELIMITER);
    byte[] stopRow = Bytes.stopKeyForPrefix(rowKeyPrefix);
    Scanner indexScan = index.scan(rowKeyPrefix, stopRow);
    return new IndexScanner(indexScan, column, value);
  }

  /**
   * Reads table rows within the given secondary index key range. If no rows are indexed, falling within the given
   * range, then a {@link co.cask.cdap.api.dataset.table.Scanner} with no results will be returned.
   *
   * @param column the column to use for the index lookup
   * @param startValue the inclusive start of the range for which rows must fall within to be returned in the scan.
   *                   {@code null} means start from first row of the table
   * @param endValue the exclusive end of the range for which rows must fall within to be returned in the scan
   *                 {@code null} means end with the last row of the table
   * @return a Scanner returning rows from the data table, whose stored value for the given column is within the the
   *         given range.
   * @throws java.lang.IllegalArgumentException if the given column is not configured for indexing.
   */
  public Scanner scanByIndex(byte[] column, @Nullable byte[] startValue, @Nullable byte[] endValue) {
    assertIndexedColumn(column);
    // KEY_DELIMITER is not used at the end of the rowKeys, because they are used for a range scan,
    // instead of a fixed-match lookup
    byte[] startRow = startValue == null ? Bytes.concat(column, KEY_DELIMITER) :
      Bytes.concat(column, KEY_DELIMITER, startValue);
    byte[] stopRow = endValue == null ? Bytes.stopKeyForPrefix(Bytes.concat(column, KEY_DELIMITER)) :
      Bytes.concat(column, KEY_DELIMITER, endValue);
    Scanner indexScan = index.scan(startRow, stopRow);
    return new IndexRangeScanner(indexScan, column, startValue, endValue);
  }

  private void assertIndexedColumn(byte[] column) {
    if (!indexedColumns.contains(column)) {
      throw new IllegalArgumentException("Column " + Bytes.toStringBinary(column) + " is not configured for indexing");
    }
  }

  /**
   * Writes a put to the data table. If any of the columns in the {@link Put} are configured to be indexed, the
   * appropriate indexes will be updated with the indexed values referencing the data table row.
   * 
   * @param put The put operation to store
   */
  @Override
  public void put(Put put) {
    // if different value exists, remove current index ref
    // add a new index ref unless same value already exists
    byte[] dataRow = put.getRow();
    // find which values need to be indexed
    Map<byte[], byte[]> putColumns = put.getValues();
    Set<byte[]> colsToIndex = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], byte[]> putEntry : putColumns.entrySet()) {
      if (indexedColumns.contains(putEntry.getKey())) {
        colsToIndex.add(putEntry.getKey());
      }
    }

    // first read the existing indexed values to find which have changed and need to be updated
    Row existingRow = table.get(dataRow, colsToIndex.toArray(new byte[colsToIndex.size()][]));
    for (Map.Entry<byte[], byte[]> entry : existingRow.getColumns().entrySet()) {
      if (!Arrays.equals(entry.getValue(), putColumns.get(entry.getKey()))) {
        index.delete(createIndexKey(dataRow, entry.getKey(), entry.getValue()), IDX_COL);
      } else {
        // value already indexed
        colsToIndex.remove(entry.getKey());
      }
    }

    // add new index entries for all values that have changed or did not exist
    for (byte[] col : colsToIndex) {
      index.put(createIndexKey(dataRow, col, putColumns.get(col)), IDX_COL, dataRow);
    }

    // store the data row
    table.put(put);
  }

  private byte[] createIndexKey(byte[] row, byte[] column, byte[] value) {
    return Bytes.concat(column, KEY_DELIMITER, value, KEY_DELIMITER, row);
  }

  @Override
  public void put(byte[] row, byte[] column, byte[] value) {
    Put put = new Put(row);
    put.add(column, value);
    put(put);
  }

  @Override
  public void put(byte[] row, byte[][] columns, byte[][] values) {
    Put put = new Put(row);
    for (int i = 0; i < columns.length; i++) {
      put.add(columns[i], values[i]);
    }
    put(put);
  }

  /**
   * Perform a delete on the data table.  Any index entries referencing the deleted row will also be removed.
   * 
   * @param delete The delete operation identifying the row and optional columns to remove
   */
  @Override
  public void delete(Delete delete) {
    if (delete.getColumns().isEmpty()) {
      // full row delete
      delete(delete.getRow());
      return;
    }
    delete(delete.getRow(), delete.getColumns().toArray(new byte[0][]));
  }

  @Override
  public void delete(byte[] row) {
    Row existingRow = table.get(row);
    if (existingRow.isEmpty()) {
      // no row to delete
      return;
    }

    // delete all index entries
    deleteIndexEntries(existingRow);

    // delete the row
    table.delete(row);
  }

  @Override
  public void delete(byte[] row, byte[] column) {
    delete(row, new byte[][]{ column });
  }

  @Override
  public void delete(byte[] row, byte[][] columns) {
    Row existingRow = table.get(row, columns);
    if (existingRow.isEmpty()) {
      // no row to delete
      return;
    }

    // delete all index entries
    deleteIndexEntries(existingRow);

    // delete the row's columns
    table.delete(row, columns);
  }

  private void deleteIndexEntries(Row existingRow) {
    byte[] row = existingRow.getRow();
    for (Map.Entry<byte[], byte[]> entry : existingRow.getColumns().entrySet()) {
      if (indexedColumns.contains(entry.getKey())) {
        index.delete(createIndexKey(row, entry.getKey(), entry.getValue()), IDX_COL);
      }
    }
  }

  /**
   * Perform a swap operation by primary key.
   * Parameters are as if they were on a non-indexed table.
   * Note that if the swap is on the secondary key column,
   * then the index must be updated; otherwise, this is a
   * pass-through to the underlying table.
   */
  @Override
  public boolean compareAndSwap(byte[] row, byte[] column, byte[] expected, byte[] newValue) {
    // if the swap is on a column other than the column key, then
    // the index is not affected - just execute the swap.
    // also, if the swap is on the index column, but the old value
    // is the same as the new value, then the index is not affected either.
    if (!indexedColumns.contains(column) ||
        Arrays.equals(expected, newValue)) {
      return table.compareAndSwap(row, column, expected, newValue);
    }

    // the swap is on the index column. it will only succeed if the current
    // value matches the expected value of the swap. if that value is not null,
    // then we must remove the row key from the index for that value.
    Delete idxDelete = null;
    if (expected != null) {
      idxDelete = new Delete(createIndexKey(row, column, expected), IDX_COL);
    }

    // if the new value is not null, then we must add the rowkey to the index
    // for that value.
    Put idxPut = null;
    if (newValue != null) {
      idxPut = new Put(createIndexKey(row, column, newValue), IDX_COL, row);
    }

    // apply all operations to both tables
    boolean success = table.compareAndSwap(row, column, expected, newValue);
    if (!success) {
      // do nothing: no changes
      return false;
    }
    if (idxDelete != null) {
      index.delete(idxDelete);
    }
    if (idxPut != null) {
      index.put(idxPut);
    }

    return true;
  }

  /**
   * Increments (atomically) the specified row and column by the specified amount, and returns the new value.
   * Note that performing this operation on an indexed column will generally have a negative impact on performance,
   * since up to three writes will need to be performed for every increment (one removing the index for the previous,
   * pre-increment value, one adding the index for the incremented value, and one for the increment itself).
   *
   * @see Table#incrementAndGet(byte[], byte[], long)
   */
  @Override
  public long incrementAndGet(byte[] row, byte[] column, long amount) {
    byte[] newValue = incrementAndGet(row, new byte[][]{ column }, new long[]{ amount }).get(column);
    return Bytes.toLong(newValue);
  }

  /**
   * Increments (atomically) the specified row and columns by the specified amounts, and returns the new values.
   * Note that performing this operation on an indexed column will generally have a negative impact on performance,
   * since up to three writes will need to be performed for every increment (one removing the index for the previous,
   * pre-increment value, one adding the index for the incremented value, and one for the increment itself).
   *
   * @see Table#incrementAndGet(byte[], byte[][], long[])
   */
  @Override
  public Row incrementAndGet(byte[] row, byte[][] columns, long[] amounts) {
    if (columns.length != amounts.length) {
      throw new IllegalArgumentException("Size of columns and amounts arguments must match");
    }

    Row existingRow = table.get(row, columns);
    byte[][] updatedValues = new byte[columns.length][];
    NavigableMap<byte[], byte[]> result = new TreeMap<>(Bytes.BYTES_COMPARATOR);

    for (int i = 0; i < columns.length; i++) {
      long existingValue = 0L;
      byte[] existingBytes = existingRow.get(columns[i]);
      if (existingBytes != null) {
        if (existingBytes.length != Bytes.SIZEOF_LONG) {
          throw new NumberFormatException("Attempted to increment a value that is not convertible to long," +
                                            " row: " + Bytes.toStringBinary(row) +
                                            " column: " + Bytes.toStringBinary(columns[i]));
        }
        existingValue = Bytes.toLong(existingBytes);
        if (indexedColumns.contains(columns[i])) {
          index.delete(createIndexKey(row, columns[i], existingBytes), IDX_COL);
        }
      }
      updatedValues[i] = Bytes.toBytes(existingValue + amounts[i]);
      result.put(columns[i], updatedValues[i]);
      if (indexedColumns.contains(columns[i])) {
        index.put(createIndexKey(row, columns[i], updatedValues[i]), IDX_COL, row);
      }
    }

    table.put(row, columns, updatedValues);
    return new Result(row, result);
  }

  /**
   * Increments (atomically) the specified row and columns by the specified amounts, and returns the new values.
   * Note that performing this operation on an indexed column will generally have a negative impact on performance,
   * since up to three writes will need to be performed for every increment (one removing the index for the previous,
   * pre-increment value, one adding the index for the incremented value, and one for the increment itself).
   *
   * @see Table#incrementAndGet(Increment)
   */
  @Override
  public Row incrementAndGet(Increment increment) {
    Map<byte[], Long> incrementValues = increment.getValues();
    Collection<Long> values = incrementValues.values();
    long[] longValues = new long[values.size()];
    int i = 0;
    for (long value : values) {
      longValues[i++] = value;
    }
    return incrementAndGet(increment.getRow(),
                           incrementValues.keySet().toArray(new byte[incrementValues.size()][]),
                           longValues);
  }


  /**
   * Increments (atomically) the specified row and column by the specified amount, without returning the new value.
   * Note that performing this operation on an indexed column throws {@link java.lang.IllegalArgumentException}.
   *
   * @see Table#increment(byte[], byte[], long)
   */
  @Override
  public void increment(byte[] row, byte[] column, long amount) {
    // read-less increments should not be used on indexed columns
    if (indexedColumns.contains(column)) {
      throw new IllegalArgumentException("Read-less increment is not supported on indexed column '"
                                           + Bytes.toStringBinary(column) + "'");
    }
    table.increment(row, column, amount);
  }

  /**
   * Increments (atomically) the specified row and columns by the specified amounts, without returning the new values.
   * Note that performing this operation on an indexed column throws {@link java.lang.IllegalArgumentException}.
   *
   * @see Table#increment(byte[], byte[][], long[])
   */
  @Override
  public void increment(byte[] row, byte[][] columns, long[] amounts) {
    // read-less increments should not be used on indexed columns
    for (byte[] col : columns) {
      if (indexedColumns.contains(col)) {
        throw new IllegalArgumentException("Read-less increment is not supported on indexed column '"
                                             + Bytes.toStringBinary(col) + "'");
      }
    }
    table.increment(row, columns, amounts);
  }

  /**
   * Increments (atomically) the specified row and columns by the specified amounts, without returning the new values.
   * Note that performing this operation on an indexed column throws {@link java.lang.IllegalArgumentException}.
   *
   * @see Table#increment(Increment)
   */
  @Override
  public void increment(Increment increment) {
    for (byte[] col : increment.getValues().keySet()) {
      if (indexedColumns.contains(col)) {
        throw new IllegalArgumentException("Read-less increment is not supported on indexed column '"
                                             + Bytes.toStringBinary(col) + "'");
      }
    }
    table.increment(increment);
  }

  @Override
  public Scanner scan(@Nullable byte[] startRow, @Nullable byte[] stopRow) {
    return table.scan(startRow, stopRow);
  }

  @Override
  public Scanner scan(Scan scan) {
    return table.scan(scan);
  }

  /* BatchReadable implementation */

  @Override
  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) {
    return table.getSplits(numSplits, start, stop);
  }

  @Override
  public Type getRecordType() {
    return table.getRecordType();
  }

  @Override
  public void write(StructuredRecord structuredRecord) throws IOException {
    table.write(structuredRecord);
  }

  @Override
  public List<Split> getSplits() {
    return table.getSplits();
  }

  @Override
  public RecordScanner<StructuredRecord> createSplitRecordScanner(Split split) {
    return table.createSplitRecordScanner(split);
  }

  @Override
  public SplitReader<byte[], Row> createSplitReader(Split split) {
    return table.createSplitReader(split);
  }

  /* BatchWritable implementation */

  @Override
  public void write(byte[] bytes, Put put) {
    put(put);
  }

  private abstract class AbstractIndexScanner implements Scanner {
    // scanner over index table
    private final Scanner baseScanner;
    private final byte[] column;

    public AbstractIndexScanner(Scanner baseScanner, byte[] column) {
      this.baseScanner = baseScanner;
      this.column = column;
    }

    /**
     * checks if a particular column value matches a criteria defined by the implementing class
     *
     * @param columnValue the column to check for a match
     * @return false to indicate to skip the corresponding row
     */
    protected abstract boolean matches(byte[] columnValue);

    @Nullable
    @Override
    public Row next() {
      // TODO: retrieve results in batches to minimize RPC overhead (requires multi-get support in table)
      // keep going until we hit a non-null, non-empty data row, or we exhaust the index
      for (Row indexRow = baseScanner.next(); indexRow != null; indexRow = baseScanner.next()) {
        byte[] rowkey = indexRow.get(IDX_COL);
        if (rowkey == null) {
          LOG.warn("Row of Indexed table '{}' is missing index column. Row key: {}", getName(), indexRow.getRow());
          continue;
        }
        byte[] columnValue = Arrays.copyOfRange(indexRow.getRow(),
                                                column.length + 1,
                                                indexRow.getRow().length - rowkey.length - 1);
        // Verify that datarow matches the expected row key to avoid issues with column name or value
        // containing the delimiter used. This is a sufficient check, as long as columns don't contain the null byte.
        if (matches(columnValue)) {
          Row row = table.get(rowkey);
          // If a column has null byte (the key delimiter) in it, then we need to check against the data row's column
          // to be sure this row isn't a false positive in the scan.
          // For reference, take a look at IndexedTableTest#testIndexKeyDelimiterAmbiguity
          if (hasColumnWithDelimiter && !Bytes.equals(row.get(column), columnValue)) {
            continue;
          }
          return row;
        }
      }
      // end of index
      return null;
    }

    @Override
    public void close() {
      baseScanner.close();
    }
  }

  // scanner that matches column values based upon exact match
  private class IndexScanner extends AbstractIndexScanner {
    private final byte[] value;

    public IndexScanner(Scanner baseScanner, byte[] column, byte[] value) {
      super(baseScanner, column);
      this.value = value;
    }

    @Override
    protected boolean matches(byte[] columnValue) {
      return Bytes.equals(columnValue, value);
    }
  }

  // scanner that matches column values based upon range
  private class IndexRangeScanner extends AbstractIndexScanner {
    private final byte[] start;
    private final byte[] end;

    public IndexRangeScanner(Scanner baseScanner, byte[] column, @Nullable byte[] start, @Nullable byte[] end) {
      super(baseScanner, column);
      this.start = start;
      this.end = end;
    }

    @Override
    protected boolean matches(byte[] columnValue) {
      return (start == null || Bytes.compareTo(columnValue, start) >= 0)
        && (end == null || Bytes.compareTo(columnValue, end) < 0);
    }
  }
}
