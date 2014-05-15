package com.continuuity.data.table;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.table.Row;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link Row}
 */
public class Result implements Row {
  private final byte[] row;
  private final Map<byte[], byte[]> columns;

  public Result(byte[] row, Map<byte[], byte[]> columns) {
    this.row = row;
    this.columns = columns;
  }

  @Override
  public byte[] getRow() {
    return row;
  }

  @Override
  public Map<byte[], byte[]> getColumns() {
    return columns;
  }

  @Override
  public boolean isEmpty() {
    return columns.isEmpty();
  }

  // column as byte[]

  @Override
  @Nullable
  public byte[] get(byte[] column) {
    return columns.get(column);
  }

  @Override
  @Nullable
  public String getString(byte[] column) {
    byte[] val = get(column);
    return val == null ? null : Bytes.toStringBinary(columns.get(column));
  }

  @Override
  @Nullable
  public Boolean getBoolean(byte[] column) {
    byte[] val = get(column);
    return val == null ? null : Bytes.toBoolean(columns.get(column));
  }

  @Override
  @Nullable
  public Short getShort(byte[] column) {
    byte[] val = get(column);
    return val == null ? null : Bytes.toShort(columns.get(column));
  }

  @Override
  @Nullable
  public Integer getInt(byte[] column) {
    byte[] val = get(column);
    return val == null ? null : Bytes.toInt(columns.get(column));
  }

  @Override
  @Nullable
  public Long getLong(byte[] column) {
    byte[] val = get(column);
    return val == null ? null : Bytes.toLong(columns.get(column));
  }

  @Override
  @Nullable
  public Float getFloat(byte[] column) {
    byte[] val = get(column);
    return val == null ? null : Bytes.toFloat(columns.get(column));
  }

  @Override
  @Nullable
  public Double getDouble(byte[] column) {
    byte[] val = get(column);
    return val == null ? null : Bytes.toDouble(columns.get(column));
  }

  @Override
  public boolean getBoolean(byte[] column, boolean defaultValue) {
    Boolean val = getBoolean(column);
    return val == null ? defaultValue : val;
  }

  @Override
  public short getShort(byte[] column, short defaultValue) {
    Short val = getShort(column);
    return val == null ? defaultValue : val;
  }

  @Override
  public int getInt(byte[] column, int defaultValue) {
    Integer val = getInt(column);
    return val == null ? defaultValue : val;
  }

  @Override
  public long getLong(byte[] column, long defaultValue) {
    Long val = getLong(column);
    return val == null ? defaultValue : val;
  }

  @Override
  public float getFloat(byte[] column, float defaultValue) {
    Float val = getFloat(column);
    return val == null ? defaultValue : val;
  }

  @Override
  public double getDouble(byte[] column, double defaultValue) {
    Double val = getDouble(column);
    return val == null ? defaultValue : val;
  }

  // column as String

  @Override
  @Nullable
  public byte[] get(String column) {
    return get(Bytes.toBytes(column));
  }

  @Override
  @Nullable
  public String getString(String column) {
    return getString(Bytes.toBytes(column));
  }

  @Override
  @Nullable
  public Boolean getBoolean(String column) {
    return getBoolean(Bytes.toBytes(column));
  }

  @Override
  @Nullable
  public Short getShort(String column) {
    return getShort(Bytes.toBytes(column));
  }

  @Override
  @Nullable
  public Integer getInt(String column) {
    return getInt(Bytes.toBytes(column));
  }

  @Override
  @Nullable
  public Long getLong(String column) {
    return getLong(Bytes.toBytes(column));
  }

  @Override
  @Nullable
  public Float getFloat(String column) {
    return getFloat(Bytes.toBytes(column));
  }

  @Override
  @Nullable
  public Double getDouble(String column) {
    return getDouble(Bytes.toBytes(column));
  }

  @Override
  public boolean getBoolean(String column, boolean defaultValue) {
    Boolean val = getBoolean(column);
    return val == null ? defaultValue : val;
  }

  @Override
  public short getShort(String column, short defaultValue) {
    Short val = getShort(column);
    return val == null ? defaultValue : val;
  }

  @Override
  public int getInt(String column, int defaultValue) {
    Integer val = getInt(column);
    return val == null ? defaultValue : val;
  }

  @Override
  public long getLong(String column, long defaultValue) {
    Long val = getLong(column);
    return val == null ? defaultValue : val;
  }

  @Override
  public float getFloat(String column, float defaultValue) {
    Float val = getFloat(column);
    return val == null ? defaultValue : val;
  }

  @Override
  public double getDouble(String column, double defaultValue) {
    Double val = getDouble(column);
    return val == null ? defaultValue : val;
  }
}
