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

package io.cdap.cdap.spi.data.sql;

import io.cdap.cdap.spi.data.InvalidFieldException;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.table.StructuredTableSchema;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.FieldType;
import io.cdap.cdap.spi.data.table.field.Fields;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * The SqlStructuredRow represents one row in the sql table.
 */
public class SqlStructuredRow implements StructuredRow {

  private final Map<String, Object> columns;
  private final Collection<Field<?>> keys;
  private final StructuredTableSchema tableSchema;

  public SqlStructuredRow(StructuredTableSchema tableSchema, Map<String, Object> columns) {
    this.tableSchema = tableSchema;
    this.columns = Collections.unmodifiableMap(new HashMap<>(columns));
    this.keys = extractKeys();
  }

  @Nullable
  @Override
  public Integer getInteger(String fieldName) throws InvalidFieldException {
    validateField(fieldName, EnumSet.of(FieldType.Type.INTEGER));
    return (Integer) columns.get(fieldName);
  }

  @Nullable
  @Override
  public Long getLong(String fieldName) throws InvalidFieldException {
    validateField(fieldName, EnumSet.of(FieldType.Type.INTEGER, FieldType.Type.LONG));
    return (Long) columns.get(fieldName);
  }

  @Nullable
  @Override
  public String getString(String fieldName) throws InvalidFieldException {
    validateField(fieldName, EnumSet.of(FieldType.Type.STRING));
    return (String) columns.get(fieldName);
  }

  @Nullable
  @Override
  public Float getFloat(String fieldName) throws InvalidFieldException {
    validateField(fieldName, EnumSet.of(FieldType.Type.FLOAT));
    return (Float) columns.get(fieldName);
  }

  @Nullable
  @Override
  public Double getDouble(String fieldName) throws InvalidFieldException {
    validateField(fieldName, EnumSet.of(FieldType.Type.FLOAT, FieldType.Type.DOUBLE));
    return (Double) columns.get(fieldName);
  }

  @Nullable
  @Override
  public byte[] getBytes(String fieldName) throws InvalidFieldException {
    validateField(fieldName, EnumSet.of(FieldType.Type.BYTES));
    return (byte[]) columns.get(fieldName);
  }

  @Override
  public Collection<Field<?>> getPrimaryKeys() {
    return keys;
  }

  private List<Field<?>> extractKeys() {
    List<Field<?>> result = new ArrayList<>();
    for (String key : tableSchema.getPrimaryKeys()) {
      // the NullPointerException should never be thrown since the primary keys must always have a type
      FieldType.Type type = tableSchema.getType(key);
      switch (Objects.requireNonNull(type)) {
        case INTEGER:
          result.add(Fields.intField(key, (int) columns.get(key)));
          break;
        case LONG:
          result.add(Fields.longField(key, (long) columns.get(key)));
          break;
        case STRING:
          result.add(Fields.stringField(key, (String) columns.get(key)));
          break;
        case BYTES:
          result.add(Fields.bytesField(key, (byte[]) columns.get(key)));
          break;
        default:
          // this should never happen since all the keys are from the table schema and should never contain other types
          throw new IllegalStateException(
            String.format("The type %s of the primary key %s is not a valid key type", type, key));
      }
    }
    return result;
  }

  private void validateField(String fieldName, Set<FieldType.Type> validTypes) throws InvalidFieldException {
    FieldType.Type type = tableSchema.getType(fieldName);
    if (type == null || !validTypes.contains(type)) {
      throw new InvalidFieldException(tableSchema.getTableId(), fieldName);
    }
  }
}
