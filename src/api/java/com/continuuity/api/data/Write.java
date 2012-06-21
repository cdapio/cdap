package com.continuuity.api.data;


public class Write implements WriteOperation {

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

  public Write(final byte [] key, final byte [] column,
      final byte [] value) {
    this(key, new byte [][] { column }, new byte [][] { value } );
  }

  public Write(final byte [] key, final byte [][] columns,
      final byte [][] values) {
    checkColumnArgs(columns, values);
    this.key = key;
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
  public static void checkColumnArgs(final byte [][] columns,
      final byte [][] values) {
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
