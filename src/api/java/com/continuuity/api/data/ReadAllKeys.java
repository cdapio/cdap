package com.continuuity.api.data;

/**
 * Read all keys and rows.
 * 
 * Supports both key-value and columnar operations.
 */
public class ReadAllKeys implements ReadOperation {

  /** The number of keys to offset by */
  private final int offset;
  
  /** The maximum number of keys to return */
  private final int limit;

  /**
   * Reads all of the keys and rows in the range specified by the given offset
   * and limit.
   * @param offset number of keys to offset by
   * @param limit maximum number of keys to return
   */
  public ReadAllKeys(int offset, int limit) {
    this.offset = offset;
    this.limit = limit;
  }

  public int getOffset() {
    return this.offset;
  }

  public int getLimit() {
    return this.limit;
  }

}
