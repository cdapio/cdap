package com.continuuity.data.operation;

import java.util.Arrays;

/**
 * This operation returns a scanner for a range of a table, optionally restricted to a subset of the columns.
 */
public class Scan extends ReadOperation {

  // the table name
  private final String table;

  // the start and the end of the input range
  private final byte[] start, stop;

  // the columns to read
  private final byte[][] columns;

  /**
   * Scan an entire table.
   * @param table the table name
   */
  public Scan(String table) {
    this(table, null, null);
  }

  /**
   * Scan a range of rows in a table.
   * @param table the table name
   * @param start the start of the range, or null to include all rows from the beginning
   * @param stop the end of the range or null to include all rows up to the end
   */
  public Scan(String table, byte[] start, byte[] stop) {
    this(table, start, stop, null);
  }

  /**
   * Scan an entire table, but read only a subset of the columns.
   * @param table the table name
   * @param columns the columns to read
   */
  public Scan(String table, byte[][] columns) {
    this(table, null, null, columns);
  }

  /**
   * Scan a range of rows in a table, reading only a subset of the columns.
   * @param table the table name
   * @param start the start of the range, or null to include all rows from the beginning
   * @param stop the end of the range or null to include all rows up to the end
   * @param columns the columns to read
   */
  public Scan(String table, byte[] start, byte[] stop, byte[][] columns) {
    this.table = table;
    this.start = start;
    this.stop = stop;
    this.columns = columns;
  }

  public String getTable() {
    return table;
  }

  public byte[] getStart() {
    return start;
  }

  public byte[] getStop() {
    return stop;
  }

  public byte[][] getColumns() {
    return columns;
  }

  @Override
  public String toString() {
    return "Scan{" +
      "table='" + table + '\'' +
      ", start=" + Arrays.toString(start) +
      ", stop=" + Arrays.toString(stop) +
      ", columns=" + Arrays.toString(columns) +
      '}';
  }
}
