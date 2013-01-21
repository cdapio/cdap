package com.continuuity.api.data.set;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;

import java.util.Map;

/**
 * This is the DataSet implementation of named tables. Other DataSets can be
 * defined by embedding instances Table (and other DataSets).
 *
 * A Table can execute operations on its data, including read, write,
 * delete etc. These operations can be performed in one of two ways:
 * <li>Synchronously: The operation is executed immediately against the
 *   data fabric, in its own transaction. This is supported for all types
 *   of operations. </li>
 * <li>Asynchronously: The operation is staged for execution as part of
 *   the transaction of the context in which this data set was
 *   instantiated (a flowlet, or a procedure). In this case,
 *   the actual execution is delegated to the context. This is useful
 *   when multiple operations, possibly over multiple table,
 *   must be performed atomically. This is only supported for write
 *   operations.</li>
 *
 * The Table relies on injection of the data fabric by the execution context.
 * (@see DataSet).
 */
public class Table extends DataSet {

  private Table delegate = null;

  /** construct by name */
  public Table(String name) {
    super(name);
  }

  /** runtime initialization, only calls the super class */
  public Table(DataSetSpecification spec) {
    super(spec);
  }

  @Override
  public DataSetSpecification.Builder configure() {
    return new DataSetSpecification.Builder(this);
  }

  /**
   * helper to return the name of the physical table. currently the same as
   * the name of the (Table) data set.
   */
  protected String tableName() {
    return this.getName();
  }

  public void setDelegate(Table table) {
    this.delegate = table;
  }

  public OperationResult<Map<byte[], byte[]>> read(@SuppressWarnings("unused") Read op) throws
      OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    return this.delegate.read(op);
  }

  public void stage(WriteOperation op) throws OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    this.delegate.stage(op);
  }

  public void exec(WriteOperation op) throws OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    this.delegate.exec(op);
  }

  /**
   * A Read operation. Reads either a list or a range of columns from one
   * row of the table.
   */
  public static class Read {
    protected byte[] row;
    protected byte[][] columns;
    protected byte[] startCol;
    protected byte[] stopCol;

    public byte[] getRow() {
      return row;
    }

    public byte[][] getColumns() {
      return columns;
    }

    public byte[] getStartCol() {
      return startCol;
    }

    public byte[] getStopCol() {
      return stopCol;
    }

    /**
     * Read a several columns
     * @param row the row key
     * @param columns an array of column keys
     */
    public Read(byte[] row, byte[][] columns) {
      this.row = row;
      this.columns = columns;
      this.startCol = this.stopCol = null;
    }

    /**
     * Read a single column
     * @param row the row key
     * @param column the column key
     */
    public Read(byte[] row, byte[] column) {
      this(row, new byte[][] { column });
    }

    /**
     * Read a consecutive range of columns
     * @param row the row key
     * @param start the column to read. If null, reading will start with the
     *              first column of the row.
     * @param stop the first column to exclude from the read. If null,
     *             the read will end with the last column in the row.
     */
    public Read(byte[] row, byte[] start, byte[] stop) {
      this.row = row;
      this.columns = null;
      this.startCol = start;
      this.stopCol = stop;
    }
  }

  /** common interface for all write operations */
  public static abstract class WriteOperation {
    byte[] row;
    public WriteOperation(byte[] row) {
      this.row = row;
    }
    public byte[] getRow() {
      return this.row;
    }
  }

  /**
   * A write to a table. It can write one more columns of a row
   */
  public static class Write extends WriteOperation {
    byte[][] columns;
    byte[][] values;

    public byte[][] getColumns() {
      return columns;
    }

    public byte[][] getValues() {
      return values;
    }

    /**
     * Write several columns. columns must have exactly the same length as
     * values, such that values[i] will be written to columns[i] of the row.
     * @param row a row key
     * @param columns an array of column keys
     * @param values an array of values to be written
     */
    public Write(byte[] row, byte[][] columns, byte[][] values) {
      super(row);
      this.columns = columns;
      this.values = values;
    }

    /**
     * Write a value to one column
     * @param row a row key
     * @param column a column key
     * @param value a new value for the column
     */
    public Write(byte[] row, byte[] column, byte[] value) {
      this(row, new byte[][] { column }, new byte[][] { value });
    }
  }

  /**
   * An Increment interprets the values of columns as 8-byte integers, and
   * increments them by given value. The operation fails if a column's
   * existing value is not exactly 8 bytes long. If one of the columns to
   * increment does not exist prior to the operation, then it will be set to
   * the value to increment.
   */
  public static class Increment extends WriteOperation {
    byte[][] columns;
    long[] values;

    public byte[][] getColumns() {
      return columns;
    }

    public long[] getValues() {
      return values;
    }

    /**
     * Increment several columns. columns must have exactly the same length as
     * values, such that the column with key columns[i] will be incremented
     * by values[i].
     * @param row the row key
     * @param columns the columns keys
     * @param values the increment values
     */
    public Increment(byte[] row, byte[][] columns, long[] values) {
      super(row);
      this.columns = columns;
      this.values = values;
    }

    /**
     * Increment a single column.
     * @param row the row key
     * @param column the column key
     * @param value the value to add
     */
    public Increment(byte[] row, byte[] column, long value) {
      this(row, new byte[][] { column }, new long[] { value });
    }
  }

  /**
   * A Delete removes one or more columns from a row. Note that to delete an
   * entire row, the caller needs to know the columns that exist.
   */
  public static class Delete extends WriteOperation {
    byte[][] columns;

    public byte[][] getColumns() {
      return columns;
    }

    /**
     * Delete several columns
     * @param row the row key
     * @param columns the column keys of the columns to be deleted
     */
    public Delete(byte[] row, byte[][] columns) {
      super(row);
      this.columns = columns;
    }

    /**
     * Delete a single column
     * @param row the row key
     * @param column the column key of the column to be deleted
     */
    public Delete(byte[] row, byte[] column) {
      this(row, new byte[][] { column });
    }
  }

  /**
   * Compare a column of a row with an expected value. If the value matches,
   * write a new value to the columns. It it does not match, the operation
   * fails.
   */
  public static class Swap extends WriteOperation {
    byte[] column;
    byte[] expected;
    byte[] value;

    public byte[] getColumn() {
      return column;
    }

    public byte[] getExpected() {
      return expected;
    }

    public byte[] getValue() {
      return value;
    }

    /**
     * Swap constructor.
     * @param row the row key
     * @param column the column key of the column to be swapped
     * @param expected the expected value. If null, then the existing value of
     *                 the column must be null, or the column must not exist.
     * @param value the new value. If null, then the column is deleted.
     */
    public Swap(byte[] row, byte[] column, byte[] expected, byte[] value) {
      super(row);
      this.column = column;
      this.expected = expected;
      this.value = value;
    }
  }

}
