package com.continuuity.api.data;

/**
 * Read the value of a single key.
 * 
 * Support only key-value operations.
 */
public class ReadKey implements ReadOperation {

  /** the name of the table */
  private final String table;

  /** The key to read */
  private final byte [] key;

  /**
   * Reads the value of the specified key.
   * @param key the key to read
   */
  public ReadKey(final byte [] key) {
    this(null, key);
  }

  /**
   * Reads the value of the specified key.
   *
   * @param table the name of the table to read from
   * @param key the key to read
   */
  public ReadKey(final String table,
                 final byte [] key) {
    this.table = table;
    this.key = key;
  }

  public String getTable() {
    return this.table;
  }

  public byte [] getKey() {
    return this.key;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Read{key=");
    sb.append(new String(key));
    sb.append("}");
    return sb.toString();
  }
}
