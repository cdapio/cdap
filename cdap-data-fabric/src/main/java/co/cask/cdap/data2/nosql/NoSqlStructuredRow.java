/*
 * Copyright © 2019 Cask Data, Inc.
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
import co.cask.cdap.spi.data.table.StructuredTableSchema;
import co.cask.cdap.spi.data.table.field.Field;
import co.cask.cdap.spi.data.table.field.FieldFactory;
import co.cask.cdap.spi.data.table.field.FieldType;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * The nosql structured row represents a row in the nosql table.
 */
public final class NoSqlStructuredRow implements StructuredRow {
  private final Row row;
  private final StructuredTableSchema tableSchema;
  private final Map<String, Object> keyFields;
  private final Collection<Field<?>> keys;

  NoSqlStructuredRow(Row row, StructuredTableSchema tableSchema) {
    this.row = row;
    this.tableSchema = tableSchema;
    this.keys = new ArrayList<>();
    this.keyFields = extractKeys();
  }

  @Nullable
  @Override
  public Integer getInteger(String fieldName) throws InvalidFieldException {
    return get(fieldName);
  }

  @Nullable
  @Override
  public Long getLong(String fieldName) throws InvalidFieldException {
    return get(fieldName);
  }

  @Nullable
  @Override
  public String getString(String fieldName) throws InvalidFieldException {
    return get(fieldName);
  }

  @Nullable
  @Override
  public Float getFloat(String fieldName) throws InvalidFieldException {
    return get(fieldName);
  }

  @Nullable
  @Override
  public Double getDouble(String fieldName) throws InvalidFieldException {
    return get(fieldName);
  }

  @Override
  public Collection<Field<?>> getPrimaryKeys() {
    return keys;
  }

  @Nullable
  @SuppressWarnings("unchecked")
  private <T> T get(String fieldName) throws InvalidFieldException {
    FieldType.Type expectedType = tableSchema.getType(fieldName);
    if (expectedType == null) {
      // Field is not present in the schema
      throw new InvalidFieldException(tableSchema.getTableId(), fieldName);
    }

    // Check if field is a key
    if (tableSchema.isPrimaryKeyColumn(fieldName)) {
      return (T) keyFields.get(fieldName);
    }

    // Field is a regular column
    return getFieldValue(fieldName, expectedType);
  }

  private Map<String, Object> extractKeys() {
    FieldFactory fieldFactory = new FieldFactory(tableSchema);
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    MDSKey.Splitter splitter = new MDSKey(row.getRow()).split();
    // extract the first part since we always have the table name as the prefix
    splitter.getString();
    for (String key : tableSchema.getPrimaryKeys()) {
      // the NullPointerException should never be thrown since the primary keys must always have a type
      FieldType.Type type = tableSchema.getType(key);
      switch (Objects.requireNonNull(type)) {
        case INTEGER:
          int intVal = splitter.getInt();
          builder.put(key, intVal);
          keys.add(fieldFactory.createIntField(key, intVal));
          break;
        case LONG:
          long longVal = splitter.getLong();
          builder.put(key, longVal);
          keys.add(fieldFactory.createLongField(key, longVal));
          break;
        case STRING:
          String stringVal = splitter.getString();
          keys.add(fieldFactory.createStringField(key, stringVal));
          builder.put(key, stringVal);
          break;
        default:
          // this should never happen since all the keys are from the table schema and should never contain other types
          throw new IllegalStateException(
            String.format("The type %s of the primary key %s is not a valid key type", type, key));
      }
    }
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private <T> T getFieldValue(String fieldName, FieldType.Type expectedType) throws InvalidFieldException {
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
