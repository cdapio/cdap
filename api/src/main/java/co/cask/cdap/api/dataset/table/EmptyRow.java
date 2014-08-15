/*
 * Copyright 2014 Cask, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.api.dataset.table;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Represents an empty row (a row with no columns).
 */
public class EmptyRow implements Row {
  private final byte[] row;

  private static final Map<byte[], byte[]> EMPTY_COLUMNS = Collections.emptyMap();

  private EmptyRow(byte[] row) {
    this.row = row;
  }

  public static Row of(byte[] row) {
    return new EmptyRow(row);
  }

  @Override
  public byte[] getRow() {
    return row;
  }

  @Override
  public Map<byte[], byte[]> getColumns() {
    return EMPTY_COLUMNS;
  }

  @Override
  public boolean isEmpty() {
    return true;
  }

  @Nullable
  @Override
  public byte[] get(byte[] column) {
    return null;
  }

  @Nullable
  @Override
  public String getString(byte[] column) {
    return null;
  }

  @Nullable
  @Override
  public Boolean getBoolean(byte[] column) {
    return null;
  }

  @Nullable
  @Override
  public Short getShort(byte[] column) {
    return null;
  }

  @Nullable
  @Override
  public Integer getInt(byte[] column) {
    return null;
  }

  @Nullable
  @Override
  public Long getLong(byte[] column) {
    return null;
  }

  @Nullable
  @Override
  public Float getFloat(byte[] column) {
    return null;
  }

  @Nullable
  @Override
  public Double getDouble(byte[] column) {
    return null;
  }

  @Override
  public boolean getBoolean(byte[] column, boolean defaultValue) {
    return defaultValue;
  }

  @Override
  public short getShort(byte[] column, short defaultValue) {
    return defaultValue;
  }

  @Override
  public int getInt(byte[] column, int defaultValue) {
    return defaultValue;
  }

  @Override
  public long getLong(byte[] column, long defaultValue) {
    return defaultValue;
  }

  @Override
  public float getFloat(byte[] column, float defaultValue) {
    return defaultValue;
  }

  @Override
  public double getDouble(byte[] column, double defaultValue) {
    return defaultValue;
  }

  @Nullable
  @Override
  public byte[] get(String column) {
    return null;
  }

  @Nullable
  @Override
  public String getString(String column) {
    return null;
  }

  @Nullable
  @Override
  public Boolean getBoolean(String column) {
    return null;
  }

  @Nullable
  @Override
  public Short getShort(String column) {
    return null;
  }

  @Nullable
  @Override
  public Integer getInt(String column) {
    return null;
  }

  @Nullable
  @Override
  public Long getLong(String column) {
    return null;
  }

  @Nullable
  @Override
  public Float getFloat(String column) {
    return null;
  }

  @Nullable
  @Override
  public Double getDouble(String column) {
    return null;
  }

  @Override
  public boolean getBoolean(String column, boolean defaultValue) {
    return defaultValue;
  }

  @Override
  public short getShort(String column, short defaultValue) {
    return defaultValue;
  }

  @Override
  public int getInt(String column, int defaultValue) {
    return defaultValue;
  }

  @Override
  public long getLong(String column, long defaultValue) {
    return defaultValue;
  }

  @Override
  public float getFloat(String column, float defaultValue) {
    return defaultValue;
  }

  @Override
  public double getDouble(String column, double defaultValue) {
    return defaultValue;
  }
}
