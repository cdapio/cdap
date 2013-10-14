package com.continuuity.api.data.dataset.table;

import com.continuuity.api.common.Bytes;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * An Increment interprets the values of columns as 8-byte integers, and
 * increments them by given value.
 * NOTE: The operation fails if a column's existing value is
 *       not exactly 8 bytes long (with {@link NumberFormatException}).
 * NOTE: If one of the columns to increment does not exist prior to the operation, then it will be set to
 *       the value to increment.
 * NOTE: Increment operation should change at least one column
 *
 */
public class Increment {
  /** row to change */
  private final byte[] row;
  /** map of column to value to increment by */
  private final Map<byte[], Long> values;

  /**
   * @return row to change
   */
  public byte[] getRow() {
    return row;
  }

  /**
   * @return map of column to value to increment by
   */
  public Map<byte[], Long> getValues() {
    return values;
  }

  // key as byte[]

  /**
   * Changes values in given row
   * @param row row to change
   */
  public Increment(byte[] row) {
    this.row = row;
    this.values = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
  }

  /**
   * Changes at least one given column in a given row
   * @param row row to change
   * @param column column to change
   * @param value value to increment by
   */
  public Increment(byte[] row, byte[] column, long value) {
    this(row);
    add(column, value);
  }

  /**
   * Adds another column to increment
   * @param column column to change
   * @param value value to increment by
   * @return instance of this {@link Increment}
   */
  public Increment add(byte[] column, long value) {
    values.put(column, value);
    return this;
  }

  // key & column as String

  /**
   * Changes values in given row
   * @param row row to change
   */
  public Increment(String row) {
    this(Bytes.toBytes(row));
  }

  /**
   * Changes at least one given column in a given row
   * @param row row to change
   * @param column column to change
   * @param value value to increment by
   */
  public Increment(String row, String column, long value) {
    this(Bytes.toBytes(row));
    add(column, value);
  }

  /**
   * Adds another column to increment
   * @param column column to change
   * @param value value to increment by
   * @return instance of this {@link Increment}
   */
  public Increment add(String column, long value) {
    return add(Bytes.toBytes(column), value);
  }
}

