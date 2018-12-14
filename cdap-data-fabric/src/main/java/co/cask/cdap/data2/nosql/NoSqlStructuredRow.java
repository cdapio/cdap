/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.data2.nosql;

import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.spi.data.InvalidFieldException;
import co.cask.cdap.spi.data.StructuredRow;
import co.cask.cdap.spi.data.table.field.FieldType;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
public final class NoSqlStructuredRow implements StructuredRow {
  private final Row row;
  private final TableSchema tableSchema;
  private final Map<String, ? super Object> keyFields;

  NoSqlStructuredRow(Row row, TableSchema tableSchema) {
    this.row = row;
    this.tableSchema = tableSchema;
    this.keyFields = new HashMap<>();
  }

  @Nullable
  @Override
  public Integer getInteger(String fieldName) throws InvalidFieldException {
    return get(fieldName, tableSchema.getType(fieldName));
  }

  @Nullable
  @Override
  public Long getLong(String fieldName) throws InvalidFieldException {
    return get(fieldName, tableSchema.getType(fieldName));
  }

  @Nullable
  @Override
  public String getString(String fieldName) throws InvalidFieldException {
    return get(fieldName, tableSchema.getType(fieldName));
  }

  @Nullable
  @Override
  public Float getFloat(String fieldName) throws InvalidFieldException {
    return get(fieldName, tableSchema.getType(fieldName));
  }

  @Nullable
  @Override
  public Double getDouble(String fieldName) throws InvalidFieldException {
    return get(fieldName, tableSchema.getType(fieldName));
  }

  private <T> T get(String fieldName, @Nullable FieldType.Type expectedType) {
    if (expectedType == null) {
      // Field is not present in the schema
      throw new InvalidFieldException(tableSchema.getTableId(), fieldName);
    }

    try {
      // Check if field is a key
      if (tableSchema.isKey(fieldName)) {
        return getKeyValue(fieldName);
      }

      // Field is a regular column
      T value = getFieldValue(fieldName, expectedType);
      if (value != null) {
        return value;
      }

      // Field is null in storage
      return null;
    } catch (IllegalArgumentException | ClassCastException e) {
      throw new InvalidFieldException(tableSchema.getTableId(), fieldName);
    }
  }

  private <T> T getKeyValue(String fieldName) {
    // Lazy extract keys on first get
    if (keyFields.isEmpty()) {
      extractKeys();
    }
    //noinspection unchecked
    return (T) keyFields.get(fieldName);
  }

  private void extractKeys() {
    MDSKey.Splitter splitter = new MDSKey(row.getRow()).split();
    for (String key : tableSchema.getPrimaryKeys()) {
      switch (tableSchema.getType(key)) {
        case INTEGER:
          keyFields.put(key, splitter.getInt());
          break;
        case LONG:
          keyFields.put(key, splitter.getLong());
          break;
        case STRING:
          keyFields.put(key, splitter.getString());
          break;
          default:
            throw new InvalidFieldException(tableSchema.getTableId(), key);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T getFieldValue(String fieldName, FieldType.Type expectedType) {
    switch (expectedType) {
      case INTEGER:
        return (T) row.getInt(fieldName);
      case LONG:
        return (T) row.getLong(fieldName);
      case FLOAT:
        return (T) row.getFloat(fieldName);
      case DOUBLE:
        return (T) row.getDouble(fieldName);
      case STRING:
        return (T) row.getString(fieldName);
        default:
          throw new InvalidFieldException(tableSchema.getTableId(), fieldName);
    }
  }
}
