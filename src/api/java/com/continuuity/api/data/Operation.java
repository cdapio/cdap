package com.continuuity.api.data;

/**
 * A read or write data operation.
 */
public interface Operation {

  public static final byte [] KV_COL = new byte [] { (byte)'c' };

  public static final byte [][] KV_COL_ARR = new byte [][] { KV_COL };

  public static final byte [][] ALL_COLS = new byte [][] { };

  /**
   * @return unique Id associated with the operation.
   */
  public long getId();
}
