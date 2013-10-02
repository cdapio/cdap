package com.continuuity.api.data.dataset.table;

import com.continuuity.api.common.Bytes;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Writes one or multiple values in columns of a row.
 * NOTE: this operation overrides existing values
 */
public class Put {
  /** row to write to */
  private final byte[] row;

  /** map of column to value to write */
  private final Map<byte[], byte[]> values;

  /**
   * @return row to write to
   */
  public byte[] getRow() {
    return row;
  }

  /**
   * @return map of column to value to write
   */
  public Map<byte[], byte[]> getValues() {
    return values;
  }

  // key as byte[]

  /**
   * Write to a specific row
   * @param row ro to write to
   */
  public Put(byte[] row) {
    this.row = row;
    this.values = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
  }

  /**
   * Write at least one value in a column of specific row
   * @param row row to write to
   * @param column column to write to
   * @param value value to write
   */
  public Put(byte[] row, byte[] column, byte[] value) {
    this(row);
    add(column, value);
  }

  /**
   * Write a value to a specific column
   * @param column column to write to
   * @param value value to write
   * @return instance of this {@link Put}
   */
  public Put add(byte[] column, byte[] value) {
    values.put(column, value);
    return this;
  }

  /**
   * Write at least one value in a column of specific row
   * @param row row to write to
   * @param column column to write to
   * @param value value to write
   */
  public Put(byte[] row, byte[] column, String value) {
    this(row);
    add(column, value);
  }

  /**
   * Write a value to a specific column
   * @param column column to write to
   * @param value value to write
   * @return instance of this {@link Put}
   */
  public Put add(byte[] column, String value) {
    return add(column, Bytes.toBytes(value));
  }

  /**
   * Write at least one value in a column of specific row
   * @param row row to write to
   * @param column column to write to
   * @param value value to write
   */
  public Put(byte[] row, byte[] column, boolean value) {
    this(row);
    add(column, value);
  }

  /**
   * Write a value to a specific column
   * @param column column to write to
   * @param value value to write
   * @return instance of this {@link Put}
   */
  public Put add(byte[] column, boolean value) {
    return add(column, Bytes.toBytes(value));
  }

  /**
   * Write at least one value in a column of specific row
   * @param row row to write to
   * @param column column to write to
   * @param value value to write
   */
  public Put(byte[] row, byte[] column, short value) {
    this(row);
    add(column, value);
  }

  /**
   * Write a value to a specific column
   * @param column column to write to
   * @param value value to write
   * @return instance of this {@link Put}
   */
  public Put add(byte[] column, short value) {
    return add(column, Bytes.toBytes(value));
  }

  /**
   * Write at least one value in a column of specific row
   * @param row row to write to
   * @param column column to write to
   * @param value value to write
   */
  public Put(byte[] row, byte[] column, int value) {
    this(row);
    add(column, value);
  }

  /**
   * Write a value to a specific column
   * @param column column to write to
   * @param value value to write
   * @return instance of this {@link Put}
   */
  public Put add(byte[] column, int value) {
    return add(column, Bytes.toBytes(value));
  }

  /**
   * Write at least one value in a column of specific row
   * @param row row to write to
   * @param column column to write to
   * @param value value to write
   */
  public Put(byte[] row, byte[] column, long value) {
    this(row);
    add(column, value);
  }

  /**
   * Write a value to a specific column
   * @param column column to write to
   * @param value value to write
   * @return instance of this {@link Put}
   */
  public Put add(byte[] column, long value) {
    return add(column, Bytes.toBytes(value));
  }

  /**
   * Write at least one value in a column of specific row
   * @param row row to write to
   * @param column column to write to
   * @param value value to write
   */
  public Put(byte[] row, byte[] column, float value) {
    this(row);
    add(column, value);
  }

  /**
   * Write a value to a specific column
   * @param column column to write to
   * @param value value to write
   * @return instance of this {@link Put}
   */
  public Put add(byte[] column, float value) {
    return add(column, Bytes.toBytes(value));
  }

  /**
   * Write at least one value in a column of specific row
   * @param row row to write to
   * @param column column to write to
   * @param value value to write
   */
  public Put(byte[] row, byte[] column, double value) {
    this(row);
    add(column, value);
  }

  /**
   * Write a value to a specific column
   * @param column column to write to
   * @param value value to write
   * @return instance of this {@link Put}
   */
  public Put add(byte[] column, double value) {
    return add(column, Bytes.toBytes(value));
  }

  // key & column as String

  /**
   * Write to a specific row
   * @param row ro to write to
   */
  public Put(String row) {
    this(Bytes.toBytes(row));
  }

  /**
   * Write at least one value in a column of specific row
   * @param row row to write to
   * @param column column to write to
   * @param value value to write
   */
  public Put(String row, String column, byte[] value) {
    this(Bytes.toBytes(row));
    add(column, value);
  }

  /**
   * Write a value to a specific column
   * @param column column to write to
   * @param value value to write
   * @return instance of this {@link Put}
   */
  public Put add(String column, byte[] value) {
    return add(Bytes.toBytes(column), value);
  }

  /**
   * Write at least one value in a column of specific row
   * @param row row to write to
   * @param column column to write to
   * @param value value to write
   */
  public Put(String row, String column, String value) {
    this(Bytes.toBytes(row));
    add(column, value);
  }

  /**
   * Write a value to a specific column
   * @param column column to write to
   * @param value value to write
   * @return instance of this {@link Put}
   */
  public Put add(String column, String value) {
    return add(Bytes.toBytes(column), Bytes.toBytes(value));
  }

  /**
   * Write at least one value in a column of specific row
   * @param row row to write to
   * @param column column to write to
   * @param value value to write
   */
  public Put(String row, String column, boolean value) {
    this(Bytes.toBytes(row));
    add(column, value);
  }

  /**
   * Write a value to a specific column
   * @param column column to write to
   * @param value value to write
   * @return instance of this {@link Put}
   */
  public Put add(String column, boolean value) {
    return add(Bytes.toBytes(column), Bytes.toBytes(value));
  }

  /**
   * Write at least one value in a column of specific row
   * @param row row to write to
   * @param column column to write to
   * @param value value to write
   */
  public Put(String row, String column, short value) {
    this(Bytes.toBytes(row));
    add(column, value);
  }

  /**
   * Write a value to a specific column
   * @param column column to write to
   * @param value value to write
   * @return instance of this {@link Put}
   */
  public Put add(String column, short value) {
    return add(Bytes.toBytes(column), Bytes.toBytes(value));
  }

  /**
   * Write at least one value in a column of specific row
   * @param row row to write to
   * @param column column to write to
   * @param value value to write
   */
  public Put(String row, String column, int value) {
    this(Bytes.toBytes(row));
    add(column, value);
  }

  /**
   * Write a value to a specific column
   * @param column column to write to
   * @param value value to write
   * @return instance of this {@link Put}
   */
  public Put add(String column, int value) {
    return add(Bytes.toBytes(column), Bytes.toBytes(value));
  }

  /**
   * Write at least one value in a column of specific row
   * @param row row to write to
   * @param column column to write to
   * @param value value to write
   */
  public Put(String row, String column, long value) {
    this(Bytes.toBytes(row));
    add(column, value);
  }

  /**
   * Write a value to a specific column
   * @param column column to write to
   * @param value value to write
   * @return instance of this {@link Put}
   */
  public Put add(String column, long value) {
    return add(Bytes.toBytes(column), Bytes.toBytes(value));
  }

  /**
   * Write at least one value in a column of specific row
   * @param row row to write to
   * @param column column to write to
   * @param value value to write
   */
  public Put(String row, String column, float value) {
    this(Bytes.toBytes(row));
    add(column, value);
  }

  /**
   * Write a value to a specific column
   * @param column column to write to
   * @param value value to write
   * @return instance of this {@link Put}
   */
  public Put add(String column, float value) {
    return add(Bytes.toBytes(column), Bytes.toBytes(value));
  }

  /**
   * Write at least one value in a column of specific row
   * @param row row to write to
   * @param column column to write to
   * @param value value to write
   */
  public Put(String row, String column, double value) {
    this(Bytes.toBytes(row));
    add(column, value);
  }

  /**
   * Write a value to a specific column
   * @param column column to write to
   * @param value value to write
   * @return instance of this {@link Put}
   */
  public Put add(String column, double value) {
    return add(Bytes.toBytes(column), Bytes.toBytes(value));
  }
}
