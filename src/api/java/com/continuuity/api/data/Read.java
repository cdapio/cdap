package com.continuuity.api.data;

import java.util.Map;

/**
 * Read the value of a key or the values of columns.
 * 
 * Supports both key-value and columnar operations.
 */
public class Read implements ReadOperation {

  /** The key/row to read */
  private final byte [] key;
  
  /** The columns to read */
  private final byte [][] columns;

  /** The result of the read, a map from columns to their values */
  private Map<byte[], byte[]> result;

  /**
   * Reads the value of the specified key.
   * 
   * Result is available via {@link #getKeyResult()}.
   * 
   * @param key the key to read
   */
  public Read(final byte [] key) {
    this(key, KV_COL_ARR);
  }

  /**
   * Reads the value of the specified column in the specified row.
   * 
   * @param row the row to be read
   * @param column the columns to be read
   */
  public Read(final byte [] row, final byte [] column) {
    this(row, new byte [][] { column } );
  }
  
  /**
   * Reads the values of the specified columns in the specified row.
   * 
   * @param row the row to be read
   * @param columns the columns to be read
   */
  public Read(final byte [] row, final byte [][] columns) {
    this.key = row;
    this.columns = columns;
  }

  public byte [] getKey() {
    return this.key;
  }

  public byte [][] getColumns() {
    return this.columns;
  }
}
