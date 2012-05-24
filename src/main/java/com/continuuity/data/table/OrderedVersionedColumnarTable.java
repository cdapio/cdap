package com.continuuity.data.table;

import java.util.Map;

import com.continuuity.data.engine.ReadPointer;

public interface OrderedVersionedColumnarTable {

  public void put(byte[] row, byte[] column, long version, byte[] value);

  public void put(byte[] row, byte[][] columns, long version, byte[][] values);

  /**
   * Deletes all columns in the specified row with a version less than or
   * equal to the specified version.
   * @param row
   * @param version
   */
  public void delete(byte[] row, long version);

  /**
   * Deletes only the specified version of the specified column, if it exists.
   * @param row
   * @param column
   * @param version
   */
  public void delete(byte[] row, byte[] column, long version);

  /**
   * Deletes all versions of the specified column that are less than or equal
   * to the specified version.
   * @param row
   * @param column
   * @param version
   */
  public void deleteAll(byte[] row, byte[] column, long version);

  public Map<byte[], byte[]> get(byte[] row, ReadPointer readPointer);

  public byte[] get(byte[] row, byte[] column, ReadPointer readPointer);

  /**
   * Returns the latest version of all columns within the range of the specified
   * start (inclusive) and stop (exclusive) columns.
   * @param row
   * @param startColumn inclusive
   * @param stopColumn exclusive
   * @param readPointer
   * @return
   */
  public Map<byte[], byte[]> get(byte[] row, byte[] startColumn,
      byte[] stopColumn, ReadPointer readPointer);

  public Map<byte[], byte[]> get(byte[] row, byte[][] columns,
      ReadPointer readPointer);

  public long increment(byte[] row, byte[] column, long amount,
      ReadPointer readPointer, long writeVersion);

  public boolean compareAndSwap(byte[] row, byte[] column,
      byte[] expectedValue, byte[] newValue, ReadPointer readPointer,
      long writeVersion);

  /**
   * Scans all columns of all rows between the specified start row (inclusive)
   * and stop row (exclusive).  Returns the latest visible version of each
   * column.
   * @param startRow
   * @param stopRow
   * @param readPointer
   * @return
   */
  public Scanner scan(byte[] startRow, byte[] stopRow,
      ReadPointer readPointer);


  /**
   * Scans the specified columns of all rows between the specified start row
   * (inclusive) and stop row (exclusive).  Returns the latest visible version
   * of each column.
   * @param startRow
   * @param stopRow
   * @param readPointer
   * @return
   */
  public Scanner scan(byte[] startRow, byte[] stopRow,
      byte[][] columns, ReadPointer readPointer);


  /**
   * Scans all columns of all rows.  Returns the latest visible version of each
   * column.
   * @param startRow
   * @param stopRow
   * @param readPointer
   * @return
   */
  public Scanner scan(ReadPointer readPointer);
}
