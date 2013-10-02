package com.continuuity.api.data.dataset.table;

import java.util.Collection;

/**
 * A Get reads one or multiple or all columns of a row
 */
public class Get extends RowColumns<Get> {
  /**
   * Get all columns of a row
   * @param row row to get
   */
  public Get(byte[] row) {
    super(row);
  }

  /**
   * Get set of columns of specific row
   * @param row row to get
   * @param columns columns to get
   */
  public Get(byte[] row, byte[]... columns) {
    super(row, columns);
  }

  /**
   * Get set of columns of specific row
   * @param row row to get
   * @param columns columns to get
   */
  public Get(byte[] row, Collection<byte[]> columns) {
    super(row, columns.toArray(new byte[columns.size()][]));
  }

  /**
   * Get all columns of a row
   * @param row row to get
   */
  public Get(String row) {
    super(row);
  }

  /**
   * Get set of columns of specific row
   * @param row row to get
   * @param columns columns to get
   */
  public Get(String row, String... columns) {
    super(row, columns);
  }

  /**
   * Get set of columns of specific row
   * @param row row to get
   * @param columns columns to get
   */
  public Get(String row, Collection<String> columns) {
    super(row, columns.toArray(new String[columns.size()]));
  }
}
