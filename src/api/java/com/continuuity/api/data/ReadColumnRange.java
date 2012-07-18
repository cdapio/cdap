package com.continuuity.api.data;

import com.google.common.base.Objects;

/**
 * Read a range of columns in a row.
 *
 * Supports only columnar operations.
 */
public class ReadColumnRange implements ReadOperation {

  /** The row */
  private final byte [] key;

  /** The start column (null for first column) */
  private final byte [] startColumn;

  /** The stop column (null for last column) */
  private final byte [] stopColumn;

  /** The maximum number of columns to return (CURRENTLY NOT SUPPORTED) */
  private final int limit;

  /**
   * Reads the range of columns in the specified row that are sorted after the
   * specified start column, inclusive.
   * @param row the row
   * @param startColumn the first column in the range to be read, inclusive
   */
  public ReadColumnRange(final byte [] row, final byte [] startColumn) {
    this(row, startColumn, null, -1);
  }

  /**
   * Reads the range of columns in the specified row that are sorted after the
   * specified start column, inclusive, and before the specified stop column,
   * exclusive.
   * @param row the row
   * @param startColumn the first column in the range to be read, inclusive,
   *                    or null to start at the beginning of the range
   * @param stopColumn the last column in the range to be read, exclusive,
   *                   or null to stop at the end of the range
   */
  public ReadColumnRange(final byte [] row, final byte [] startColumn,
      final byte [] stopColumn) {
    this(row, startColumn, stopColumn, -1);
  }

  /**
   * Reads the range of columns in the specified row that are sorted after the
   * specified start column, inclusive, and before the specified stop column,
   * exclusive.
   *
   * Currently private because limit is unsupported.
   *
   * @param row the row
   * @param startColumn the first column in the range to be read, inclusive,
   *                    or null to start at the beginning of the range
   * @param stopColumn the last column in the range to be read, exclusive,
   *                   or null to stop at the end of the range
   * @param limit the maximum number of columns to return
   */
  private ReadColumnRange(final byte [] row, final byte [] startColumn,
      final byte [] stopColumn, int limit) {
    this.key = row;
    this.startColumn = startColumn;
    this.stopColumn = stopColumn;
    this.limit = limit;
  }

  public byte [] getKey() {
    return this.key;
  }

  public byte [] getStartColumn() {
    return this.startColumn;
  }

  public byte [] getStopColumn() {
    return this.stopColumn;
  }

  public int getLimit() {
    return this.limit;
  }

  public String toString() {
    return Objects.toStringHelper(this).
        add("key", new String(this.key)).
        add("start", this.startColumn == null
            ? "null" : new String(this.startColumn)).
        add("stop", this.stopColumn == null
            ? "null" : new String(this.stopColumn)).
        add("limit", Integer.toString(this.limit)).
        toString();
  }

}
