package com.continuuity.api.dataset.table;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Represents one row in a table with a subset of columns.
 * NOTE: Depending on the operation that returns {@link Row} as a result, it may contain all or just a subset of column
 *       values.
 */
public interface Row {
  /**
   * @return key of this row
   */
  byte[] getRow();

  /**
   * @return map of column to value of this row
   */
  Map<byte[], byte[]> getColumns();

  /**
   * @return true when has no column values, false otherwise
   */
  boolean isEmpty();

  /**
   * @param column column to get value of
   * @return value of a column or {@code null} if column is not in a subset of columns of this {@link Row}
   */
  @Nullable
  byte[] get(byte[] column);

  /**
   * @param column column to get value of
   * @return value of a column or {@code null} if column is not in a subset of columns of this {@link Row}
   */
  @Nullable
  String getString(byte[] column);

  /**
   * @param column column to get value of
   * @return value of a column or {@code null} if column is not in a subset of columns of this {@link Row}
   */
  @Nullable
  Boolean getBoolean(byte[] column);

  /**
   * @param column column to get value of
   * @return value of a column or {@code null} if column is not in a subset of columns of this {@link Row}
   */
  @Nullable
  Short getShort(byte[] column);

  /**
   * @param column column to get value of
   * @return value of a column or {@code null} if column is not in a subset of columns of this {@link Row}
   */
  @Nullable
  Integer getInt(byte[] column);

  /**
   * @param column column to get value of
   * @return value of a column or {@code null} if column is not in a subset of columns of this {@link Row}
   */
  @Nullable
  Long getLong(byte[] column);

  /**
   * @param column column to get value of
   * @return value of a column or {@code null} if column is not in a subset of columns of this {@link Row}
   */
  @Nullable
  Float getFloat(byte[] column);

  /**
   * @param column column to get value of
   * @return value of a column or {@code null} if column is not in a subset of columns of this {@link Row}
   */
  @Nullable
  Double getDouble(byte[] column);

  /**
   * @param column column to get value of
   * @param defaultValue default value to use
   * @return value of a column or given default value if column is not in a subset of columns of this {@link Row}
   */
  boolean getBoolean(byte[] column, boolean defaultValue);

  /**
   * @param column column to get value of
   * @param defaultValue default value to use
   * @return value of a column or given default value if column is not in a subset of columns of this {@link Row}
   */
  short getShort(byte[] column, short defaultValue);

  /**
   * @param column column to get value of
   * @param defaultValue default value to use
   * @return value of a column or given default value if column is not in a subset of columns of this {@link Row}
   */
  int getInt(byte[] column, int defaultValue);

  /**
   * @param column column to get value of
   * @param defaultValue default value to use
   * @return value of a column or given default value if column is not in a subset of columns of this {@link Row}
   */
  long getLong(byte[] column, long defaultValue);

  /**
   * @param column column to get value of
   * @param defaultValue default value to use
   * @return value of a column or given default value if column is not in a subset of columns of this {@link Row}
   */
  float getFloat(byte[] column, float defaultValue);

  /**
   * @param column column to get value of
   * @param defaultValue default value to use
   * @return value of a column or given default value if column is not in a subset of columns of this {@link Row}
   */
  double getDouble(byte[] column, double defaultValue);

  /**
   * @param column column to get value of
   * @return value of a column or {@code null} if column is not in a subset of columns of this {@link Row}
   */
  @Nullable
  byte[] get(String column);

  /**
   * @param column column to get value of
   * @return value of a column or {@code null} if column is not in a subset of columns of this {@link Row}
   */
  @Nullable
  String getString(String column);

  /**
   * @param column column to get value of
   * @return value of a column or {@code null} if column is not in a subset of columns of this {@link Row}
   */
  @Nullable
  Boolean getBoolean(String column);

  /**
   * @param column column to get value of
   * @return value of a column or {@code null} if column is not in a subset of columns of this {@link Row}
   */
  @Nullable
  Short getShort(String column);

  /**
   * @param column column to get value of
   * @return value of a column or {@code null} if column is not in a subset of columns of this {@link Row}
   */
  @Nullable
  Integer getInt(String column);

  /**
   * @param column column to get value of
   * @return value of a column or {@code null} if column is not in a subset of columns of this {@link Row}
   */
  @Nullable
  Long getLong(String column);

  /**
   * @param column column to get value of
   * @return value of a column or {@code null} if column is not in a subset of columns of this {@link Row}
   */
  @Nullable
  Float getFloat(String column);

  /**
   * @param column column to get value of
   * @return value of a column or {@code null} if column is not in a subset of columns of this {@link Row}
   */
  @Nullable
  Double getDouble(String column);

  /**
   * @param column column to get value of
   * @param defaultValue default value to use
   * @return value of a column or given default value if column is not in a subset of columns of this {@link Row}
   */
  boolean getBoolean(String column, boolean defaultValue);

  /**
   * @param column column to get value of
   * @param defaultValue default value to use
   * @return value of a column or given default value if column is not in a subset of columns of this {@link Row}
   */
  short getShort(String column, short defaultValue);

  /**
   * @param column column to get value of
   * @param defaultValue default value to use
   * @return value of a column or given default value if column is not in a subset of columns of this {@link Row}
   */
  int getInt(String column, int defaultValue);

  /**
   * @param column column to get value of
   * @param defaultValue default value to use
   * @return value of a column or given default value if column is not in a subset of columns of this {@link Row}
   */
  long getLong(String column, long defaultValue);

  /**
   * @param column column to get value of
   * @param defaultValue default value to use
   * @return value of a column or given default value if column is not in a subset of columns of this {@link Row}
   */
  float getFloat(String column, float defaultValue);

  /**
   * @param column column to get value of
   * @param defaultValue default value to use
   * @return value of a column or given default value if column is not in a subset of columns of this {@link Row}
   */
  double getDouble(String column, double defaultValue);
}
