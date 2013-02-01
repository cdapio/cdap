package com.continuuity.data.operation;

/**
 * Read the value of a single key.
 * 
 * Support only key-value operations.
 */
public class ReadKey implements ReadOperation {

  /** Unique id for the operation */
  private final long id;

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
    this(OperationBase.getId(), table, key);
  }

  /**
   * Reads the value of the specified key.
   *
   * @param id explicit unique id of this operation
   * @param table the name of the table to read from
   * @param key the key to read
   */
  public ReadKey(final long id,
                 final String table,
                 final byte [] key) {
    this.id = id;
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

  @Override
  public long getId() {
    return id;
  }
}
