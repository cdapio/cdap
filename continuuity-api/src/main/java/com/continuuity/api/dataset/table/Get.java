package com.continuuity.api.dataset.table;

import java.util.Collection;

/**
 * A Get reads one, multiple, or all columns of a row.
 */
public class Get extends RowColumns<Get> {
  /**
   * Get all of the columns of a row.
   * @param row Row to get.
   */
  public Get(byte[] row) {
    super(row);
  }

  /**
   * Get a set of columns of a row.
   * @param row Row to get.
   * @param columns Columns to get.
   */
  public Get(byte[] row, byte[]... columns) {
    super(row, columns);
  }

  /**
   * Get a set of columns of a row.
   * @param row Row to get.
   * @param columns Columns to get.
   */
  public Get(byte[] row, Collection<byte[]> columns) {
    super(row, columns.toArray(new byte[columns.size()][]));
  }

  /**
   * Get all of the columns of a row.
   * @param row Row to get.
   */
  public Get(String row) {
    super(row);
  }

  /**
   * Get a set of columns of a row.
   * @param row Row to get.
   * @param columns Columns to get.
   */
  public Get(String row, String... columns) {
    super(row, columns);
  }

  /**
   * Get a set of columns of a row.
   * @param row row to get
   * @param columns columns to get
   */
  public Get(String row, Collection<String> columns) {
    super(row, columns.toArray(new String[columns.size()]));
  }
}
