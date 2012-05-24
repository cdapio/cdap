/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.table;

import java.util.Map;

import com.continuuity.data.engine.ReadPointer;

/**
 * 
 */
public interface VersionedTable extends SimpleTable {
  
  public void put(byte [] row, byte [] column, long version, byte [] value);
  
  public void put(byte [] row, byte [][] columns, long [] versions,
      byte [][] values);

  /**
   * Deletes all versions of all existing columns with a delete marker with the
   * specified version.
   * @param row
   * @param version
   */
  public void deleteAll(byte[] row, byte [] column, long version);
  
  /**
   * Deletes the specified column at the specified version.
   * @param row
   * @param column
   * @param version
   */
  public void delete(byte [] row, byte [] column, long version);
  
  /**
   * Gets the latest version of the specified column that has a version less
   * than or equal to the specified max version.
   * @param row
   * @param column
   * @param maxVersion
   * @return
   */
  public byte [] get(byte [] row, byte [] column, ReadPointer readPointer);

  /**
   * Gets the latest version of each of the specified columns that has a version
   * less than or equal to the specified max version.
   * @param row
   * @param columns
   * @param readPointer
   * @return
   */
  public Map<byte[],byte[]> get(byte [] row, byte [][] columns,
      ReadPointer readPointer);

  /**
   * Get all versions of the specified column that have a version less than or
   * equal to the specified max version.  Returned map is special in that it is
   * in descending numeric order (not default ascending).
   * @param row
   * @param column
   * @param readPointer
   * @return
   */
  public Map<Long, byte[]> getAllVersions(byte [] row, byte [] column,
      ReadPointer readPointer);


  /**
   * Get all versions of the specified columns that have a version greater or
   * equal to the specified max version.  Returned submap is special in that it
   * is in descending numeric order (not default ascending).  First map is
   * in normal ascending binary order.
   * @param row
   * @param column
   * @param readPointer
   * @return
   */
  public Map<byte[], Map<Long, byte[]>> getAllVersions(byte [] row,
      byte [][] columns, ReadPointer readPointer);
  
  public long increment(byte[] row, byte[] column, long amount,
      ReadPointer readPointer, long writeVersion);

  public boolean compareAndSwap(byte[] row, byte[] column,
      byte[] expectedValue, byte[] newValue,
      ReadPointer readPointer, long writeVersion);
}
