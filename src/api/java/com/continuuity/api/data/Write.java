package com.continuuity.api.data;

/**
 * Write the value of a key or the values of columns in a row.
 * 
 * Supports both key-value and columnar operations.
 */
public class Write implements WriteOperation {

  /** the name of the table */
  private final String table;

  /** The key/row being written to */
  private final byte [] key;
  
  /** The columns being written */
  private final byte [][] columns;
  
  /** The values being written */
  private final byte [][] values;

  /**
   * Writes the specified value for the specified key to the default table
   *
   * This is a key-value operation.
   *
   * @param key the row key to write to
   * @param value the value to write
   */
  public Write(final byte [] key,
               final byte [] value) {
    this((String)null, key, value);
  }

  /**
   * Writes the specified value for the specified key to the specified table
   *
   * This is a key-value operation.
   *
   * @param table the table to write to
   * @param key the row key to write to
   * @param value the value to write
   */
  public Write(final String table,
               final byte [] key,
               final byte [] value) {
    this(table, key, KV_COL_ARR, new byte [][] { value });
  }

  /**
   * Writes the specified value for the specified column in the specified row
   * to the default table.
   *
   * This is a columnar operation.
   *
   * @param row the row key to write to
   * @param column the single column to write
   * @param value the value to write to that column
   */
  public Write(final byte [] row,
               final byte [] column,
               final byte [] value) {
    this(null, row, column, value);
  }

  /**
   * Writes the specified value for the specified column in the specified row
   * to the specified table.
   *
   * This is a columnar operation.
   *
   * @param table the table to write to
   * @param row the row key to write to
   * @param column the single column to write
   * @param value the value to write to that column
   */
  public Write(final String table,
               final byte [] row,
               final byte [] column,
               final byte [] value) {
    this(table, row, new byte [][] { column }, new byte [][] { value } );
  }

  /**
   * Writes the specified values for the specified columns in the specified row.
   *
   * This is a columnar operation.
   *
   * @param row the row key to write to
   * @param columns the columns to write
   * @param values the values to write to the columns, in the same order
   */
  public Write(final byte [] row,
               final byte [][] columns,
               final byte [][] values) {
    this(null, row, columns, values);
  }

  /**
   * Writes the specified values for the specified columns in the specified row.
   *
   * This is a columnar operation.
   *
   * @param table the table to write to
   * @param row the row key to write to
   * @param columns the columns to write
   * @param values the values to write to the columns, in the same order
   */
  public Write(final String table,
               final byte [] row,
               final byte [][] columns,
               final byte [][] values) {
    checkColumnArgs(columns, values);
    this.table = table;
    this.key = row;
    this.columns = columns;
    this.values = values;
  }

  public String getTable() {
    return this.table;
  }

  @Override
  public byte [] getKey() {
    return this.key;
  }
 
  public byte [][] getColumns() {
    return this.columns;
  }
 
  public byte [][] getValues() {
    return this.values;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Write{key=");
    sb.append(new String(key));
    sb.append(", [");
    for (int i=0; i<columns.length; i++) {
      if (i != 0) sb.append(",");
      sb.append("(col=");
      sb.append(new String(columns[i]));
      sb.append(",val=");
      sb.append(new String(values[i]));
      sb.append(")");
    }
    sb.append("]}");
    return sb.toString();
  }

  @Override
  public int getPriority() {
    return 1;
  }

  /**
   * Checks the specified columns and values arguments for validity.
   * @param columns the columns to write to
   * @param values the values to write
   * @throws IllegalArgumentException if no columns specified, no values
   *    specified, or number of columns does not match number of values
   */
  public static void checkColumnArgs(final Object [] columns,
      final Object [] values) {
    if (columns == null || columns.length == 0)
      throw new IllegalArgumentException("Must contain at least one column");
    if (values == null || values.length == 0)
      throw new IllegalArgumentException("Must contain at least one value");
    if (columns.length != values.length)
      throw new IllegalArgumentException("Number of columns (" +
          columns.length + ") does not match number of values (" +
          values.length + ")");
  }
}
