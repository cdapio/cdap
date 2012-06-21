package com.continuuity.api.data;

/**
 * Operations are the core building blocks of all data access.
 *
 */
public interface Operation {

  public static final byte [] KV_COL = new byte [] { (byte)'c' };

  public static final byte [][] KV_COL_ARR = new byte [][] { KV_COL };

  public static final byte [][] ALL_COLS = new byte [][] { };

}
