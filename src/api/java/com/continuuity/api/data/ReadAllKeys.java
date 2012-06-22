package com.continuuity.api.data;

import java.util.List;

/**
 * Read all keys and rows.
 * 
 * Supports both key-value and columnar operations.
 */
public class ReadAllKeys implements ReadOperation<List<byte[]>> {

  /** The number of keys to offset by */
  private final int offset;
  
  /** The maximum number of keys to return */
  private final int limit;

  /** The resulting list of keys */
  private List<byte[]> result;

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

  @Override
  public void setResult(List<byte[]> result) {
    this.result = result;
  }

  @Override
  public List<byte[]> getResult() {
    return this.result;
  }

  public int getOffset() {
    return this.offset;
  }

  public int getLimit() {
    return this.limit;
  }

}
