package com.continuuity.data.operation;

/**
 * A read or write data operation.
 */
public interface Operation {

  // this is the same as in KeyValueTable!
  public static final byte [] KV_COL = { 'c' };

  public static final byte [][] KV_COL_ARR = { KV_COL };

  /**
   * @return unique Id associated with the operation.
   */
  public long getId();
}
