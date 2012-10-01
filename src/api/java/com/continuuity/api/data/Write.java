package com.continuuity.api.data;

/**
 * Write the value of a key or the values of columns in a row.
 * 
 * Supports both key-value and columnar operations.
 */
public class Write implements WriteOperation {

  /** Unique id for the operation */
  private final long id = OperationBase.getId();

  /** The key/row being written to */
  private final byte [] key;
  
  /** The columns being written */
  private final byte [][] columns;
  
  /** The values being written */
  private final byte [][] values;
 
  /**
   * Writes the specified value for the specified key.
   * 
   * This is a key-value operation.
   * 
   * @param key
   * @param value
   */
  public Write(final byte [] key, final byte [] value) {
    this(key, KV_COL_ARR, new byte [][] { value });
  }

  /**
   * Writes the specified value for the specified column in the specified row.
   * 
   * This is a columnar operation.
   * 
   * @param row
   * @param column
   * @param value
   */
  public Write(final byte [] row, final byte [] column,
      final byte [] value) {
    this(row, new byte [][] { column }, new byte [][] { value } );
  }

  /**
   * Writes the specified values for the specified columns in the specified row.
   * 
   * This is a columnar operation.
   * 
   * @param row
   * @param columns
   * @param values
   */
  public Write(final byte [] row, final byte [][] columns,
      final byte [][] values) {
    checkColumnArgs(columns, values);
    this.key = row;
    this.columns = columns;
    this.values = values;
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
   * @param columns
   * @param values
   * @throws IllegalArgumentException if no columns specified
   * @throws IllegalArgumentException if no values specified
   * @throws IllegalArgumentException if number of columns does not match number
   *                                  of values
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

  @Override
  public long getId() {
    return id;
  }
}
