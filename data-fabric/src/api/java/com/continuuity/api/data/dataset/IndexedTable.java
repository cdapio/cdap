package com.continuuity.api.data.dataset;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.StatusCode;
import com.continuuity.api.data.dataset.table.Delete;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Swap;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.Write;

import java.util.Arrays;
import java.util.Map;

/**
 * This data set implements a table that can be accessed via secondary key.
 * The secondary key need not be unique, but a read by secondary key will
 * only return the value with the least primary key.
 *
 * This data set uses two tables - the actual data table,
 * and a second table for the index. All operations are performed
 * asynchronously, as part of the enclosing transaction (some operations
 * require multiple writes, and we want to make sure they are all committed
 * together).
 */
public class IndexedTable extends DataSet {

  // the names of the two undelying tables
  private String tableName, indexName;
  // the two underlying tables
  private Table table, index;
  // the secondary index column
  private byte[] column;

  // the property name for the secondary index column in the data set spec
  private String indexColumnProperty = "colum";

  // Helper method for both constructors to set the names of the underlying two tables
  private void init(String name, byte[] column) {
    this.tableName = "t_" + name;
    this.indexName = "i_" + name;
    this.column = column;
  }

  /**
   * Runtime constructor from data set spec.
   */
  @SuppressWarnings("unused")
  public IndexedTable(DataSetSpecification spec) {
    super(spec);
    this.init(this.getName(), spec.getProperty(indexColumnProperty).getBytes());
    this.table = new Table(spec.getSpecificationFor(this.tableName));
    this.index = new Table(spec.getSpecificationFor(this.indexName));
  }

  /**
   * Configuration time constructor.
   * @param name the name of the table
   * @param columnToIndex the name of the secondary index column
   */
  public IndexedTable(String name, byte[] columnToIndex) {
    super(name);
    this.init(name, columnToIndex);
    this.table = new Table(this.tableName);
    this.index = new Table(this.indexName);
  }

  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this).
        property(indexColumnProperty, new String(this.column)).
        dataset(this.table.configure()).
        dataset(this.index.configure()).
        create();
  }

  // the value in the index. the index will have a row for every secondary
  // key that exists. That row has a column with the column key of the row
  // key of every row with that secondary key. The column must have a value,
  // we use the value 'x', but iy could be any value.
  static final byte[] EXISTS = { 'x' };

  /**
   * Read by primary key.
   * @param read the read operation, as if it were on a non-indexed table
   * @return the result of the read on the underlying primary table
   * @throws OperationException if the operation fails
   */
  public OperationResult<Map<byte[], byte[]>> read(Read read)
      throws OperationException {
    return this.table.read(read);
  }

  /**
   * Read by secondary key.
   * @param read The read operation, as if it were on the non-indexed table,
   *             but with the secondary key as the row key of the Read.
   * @return an empty result if no row has that secondary key. If there is a
   * matching row, the result is the same as a read with the row key of the
   * first row that has this secondary key. The columns are the same as if
   * the read was performed on a non-indexed table.
   * @throws OperationException if the operation goes wrong
   */
  public OperationResult<Map<byte[], byte[]>> readBy(Read read)
      throws OperationException {
    // read the entire row of the index for the given key
    Read idxRead = new Read(read.getRow(), null, null);
    OperationResult<Map<byte[], byte[]>> result = this.index.read(idxRead);

    // if the index has no match, return nothing
    if (result.isEmpty()) {
      return result;
    }
    // iterate over all columns in the index result:
    // each of them represents a row key in the main table
    for (byte[] column : result.getValue().keySet()) {
      if (Arrays.equals(EXISTS, result.getValue().get(column))) {
        // construct a new read with this column as the row key
        Read tableRead = read.getColumns() == null
            ? new Read(column, read.getStartCol(), read.getStopCol())
            : new Read(column, read.getColumns());
        // issue that read against the main table
        OperationResult<Map<byte[], byte[]>> tableResult =
            this.table.read(tableRead);
        // if this yields something, return it
        if (!tableResult.isEmpty()) {
          return tableResult;
        }
      }
    }
    // nothing found - return empty result
    return new OperationResult<Map<byte[], byte[]>>(StatusCode.ENTRY_NOT_FOUND);
  }

  /**
   * A write to an indexed table. This is the same as on an indexed table,
   * except that additional work is done to maintain the index.
   * @param write The write operatiojn
   * @throws OperationException if the operation goes wrong
   */
  public void write(Write write) throws OperationException {
    // first read the existing row to find its current value of the index col
    Read firstRead = new Read(write.getRow(), this.column);
    OperationResult<Map<byte[], byte[]>> firstResult =
        this.table.read(firstRead);
    byte[] oldSecondaryKey = null;
    if (!firstResult.isEmpty()) {
      oldSecondaryKey = firstResult.getValue().get(this.column);
    }

    // find out whether the write contains a new value for the index column
    byte[] newSecondaryKey = null;
    for (int i = 0; i < write.getColumns().length; i++) {
      if (Arrays.equals(this.column, write.getColumns()[i])) {
        newSecondaryKey = write.getValues()[i];
        break;
      }
    }

    boolean keyMatches = Arrays.equals(oldSecondaryKey, newSecondaryKey);
    // if there is an existing row with a value for the index column,
    // and that value is different from the new value to be written,
    // then we must remove that the row key from the index for that value;
    Delete idxDelete = null;
    if (oldSecondaryKey != null && !keyMatches) {
      idxDelete = new Delete(oldSecondaryKey, write.getRow());
    }

    // and we only need to write to index if the row is new or gets a new
    // value for the index column;
    Write idxWrite = null;
    if (newSecondaryKey != null && !keyMatches) {
      idxWrite = new Write(newSecondaryKey, write.getRow(), EXISTS);
    }

    // apply all operations to both tables
    this.table.write(write);
    if (idxDelete != null) {
      this.index.write(idxDelete);
    }
    if (idxWrite != null) {
      this.index.write(idxWrite);
    }
  }

  /**
   * Perform a delete by primary key.
   * @param delete The delete operation, as if it were on a non-indexed table
   * @throws OperationException if the operation goes wrong
   */
  public void delete(Delete delete) throws OperationException {
    // first read the existing row to find its current value of the index col
    Read firstRead = new Read(delete.getRow(), this.column);
    OperationResult<Map<byte[], byte[]>> firstResult =
        this.table.read(firstRead);
    byte[] oldSecondaryKey = null;
    if (!firstResult.isEmpty()) {
      oldSecondaryKey = firstResult.getValue().get(this.column);
    }

    // if there is an existing row with a value for the index column,
    // then we must remove that the row key from the index for that value;
    Delete idxDelete = null;
    if (oldSecondaryKey != null) {
      idxDelete = new Delete(oldSecondaryKey, delete.getRow());
    }

    // apply all operations to both tables
    this.table.write(delete);
    if (idxDelete != null) {
      this.index.write(idxDelete);
    }
  }

  /**
   * Perform a swap operation by primary key.
   * @param swap The swap operation, as if it were on a non-indexed table.
   *             Note that if the swap is on the secondary key column,
   *             then the index must be updated; otherwise this is a
   *             pass-through to the underlying table.
   * @throws OperationException if the operation goes wrong
   */
  public void swap(Swap swap) throws OperationException {
    // if the swap is on a column other than the column key, then
    // the index is not affected - just execute the swap.
    // also, if the swap is on the index column, but the old value
    // is the same as the new value, then the index is not affected either.
    if (!Arrays.equals(this.column, swap.getColumn()) ||
        Arrays.equals(swap.getExpected(), swap.getValue())) {
      this.table.write(swap);
      return;
    }

    // the swap is on the index column. it will only succeed if the current
    // value matches the expected value of the swap. if that value is not null,
    // then we must remove the row key from the index for that value.
    Delete idxDelete = null;
    if (swap.getExpected() != null) {
      idxDelete = new Delete(swap.getExpected(), swap.getRow());
    }

    // if the new value is not null, then we must add the rowkey to the index
    // for that value.
    Write idxWrite = null;
    if (swap.getValue() != null) {
      idxWrite = new Write(swap.getValue(), swap.getRow(), EXISTS);
    }

    // apply all operations to both tables
    this.table.write(swap);
    if (idxDelete != null) {
      this.index.write(idxDelete);
    }
    if (idxWrite != null) {
      this.index.write(idxWrite);
    }
  }

  /**
   * Perform an increment operation by primary key.
   * @param increment The increment operation, as if it were on a non-indexed table.
   *             Note that if the increment is on the secondary key column,
   *             then the index must be updated; otherwise this is a
   *             pass-through to the underlying table.
   * @throws OperationException if the operation goes wrong
   */
  public void increment(Increment increment) throws OperationException {
    // if the increment is on columns other than the index, just pass
    // it through to the table - the index is not affected
    Long indexIncrement = null;
    for (int i = 0; i < increment.getColumns().length; ++i) {
      if (Arrays.equals(this.column, increment.getColumns()[i])) {
        indexIncrement = increment.getValues()[i];
        break;
      }
    }
    if (indexIncrement == null) {
      // note this only adds the increment to the current xaction, it may be deferred
      this.table.write(increment);
      return;
    }

    // index column is affected. Perform the increment synchronously
    Long newIndexValue = this.table.incrementAndGet(increment).get(this.column);
    if (newIndexValue == null) {
      // should never happen (we checked that it was in the increment columns)
      // but if it does, we are done;
      return;
    }

    // delete the old secondary key from the index
    byte[] oldSecondaryKey = Bytes.toBytes(newIndexValue - indexIncrement);
    this.table.write(new Delete(oldSecondaryKey, increment.getRow()));

    // add the new secondary key to the index
    byte[] newSecondaryKey = Bytes.toBytes(newIndexValue);
    this.table.write(new Write(newSecondaryKey, increment.getRow(), EXISTS));
  }

}
