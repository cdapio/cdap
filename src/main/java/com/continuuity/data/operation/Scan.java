package com.continuuity.data.operation;

import java.util.Arrays;

/**
 * This operation returns a scanner for a range of a table, optionally restricted to a subset of the columns.
 */
public class Scan extends ReadOperation {

  // the table name
  private final String table;

  // the start and the end of the input range
  private final byte[] startRow, stopRow;

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
   * @param startRow the start of the range, or null to include all rows from the beginning
   * @param stopRow the end of the range or null to include all rows up to the end
   */
  public Scan(String table, byte[] startRow, byte[] stopRow) {
    this(table, startRow, stopRow, null);
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
   * @param startRow the start of the range, or null to include all rows from the beginning
   * @param stopRow the end of the range or null to include all rows up to the end
   * @param columns the columns to read
   */
  public Scan(String table, byte[] startRow, byte[] stopRow, byte[][] columns) {
    this.table = table;
    this.startRow = startRow;
    this.stopRow = stopRow;
    this.columns = columns;
  }

  public String getTable() {
    return table;
  }

  public byte[] getStartRow() {
    return startRow;
  }

  public byte[] getStopRow() {
    return stopRow;
  }

  public byte[][] getColumns() {
    return columns;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    char sep = '[';
    for (byte[] column : this.columns) {
      builder.append(sep);
      builder.append(new String(column));
      sep = ',';
    }
    builder.append(']');
    String columnsStr = builder.toString();
    return "Scan{" +
      "table='" + table + '\'' +
      ", startRow=" + Arrays.toString(startRow) +
      ", stop=" + Arrays.toString(stopRow) +
      ", columns=" + columnsStr +
      '}';
  }
}
