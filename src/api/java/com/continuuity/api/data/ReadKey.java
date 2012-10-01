package com.continuuity.api.data;

/**
 * Read the value of a single key.
 * 
 * Support only key-value operations.
 */
public class ReadKey implements ReadOperation {

  /** Unique id for the operation */
  private final long id = OperationBase.getId();

  /** The key to read */
  private final byte [] key;

  /**
   * Reads the value of the specified key.
   * @param key the key to read
   */
  public ReadKey(final byte [] key) {
    this.key = key;
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
