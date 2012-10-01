package com.continuuity.api.data;

import com.google.common.base.Objects;

/**
 * Read all keys and rows.
 *
 * Supports both key-value and columnar operations.
 */
public class ReadAllKeys implements ReadOperation {

  /** Unique id for the operation */
  private final long id = OperationBase.getId();

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

  public String toString() {
    return Objects.toStringHelper(this)
        .add("offset", Integer.toString(offset))
        .add("limit", Integer.toString(limit))
        .toString();
  }

  @Override
  public long getId() {
    return id;
  }
}
