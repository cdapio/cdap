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

package co.cask.cdap.data2.sql;

import co.cask.cdap.spi.data.InvalidFieldException;
import co.cask.cdap.spi.data.StructuredRow;
import co.cask.cdap.spi.data.table.TableSchema;
import co.cask.cdap.spi.data.table.field.FieldType;
import com.google.common.collect.ImmutableSet;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 *
 */
public class SqlStructuredRow implements StructuredRow {

  private final Map<String, Object> columns;
  private final TableSchema tableSchema;

  public SqlStructuredRow(TableSchema tableSchema) {
    this.tableSchema = tableSchema;
    this.columns = new HashMap<>();
  }

  public <T> void add(String columnName, T data) {
    // only put if the field is not null
    if (data != null) {
      columns.put(columnName, data);
    }
  }

  @Nullable
  @Override
  public Integer getInteger(String fieldName) throws InvalidFieldException {
    validateField(fieldName, Collections.singleton(FieldType.Type.INTEGER));
    return (Integer) columns.get(fieldName);
  }

  @Nullable
  @Override
  public Long getLong(String fieldName) throws InvalidFieldException {
    validateField(fieldName, ImmutableSet.of(FieldType.Type.INTEGER, FieldType.Type.LONG));
    return (Long) columns.get(fieldName);
  }

  @Nullable
  @Override
  public String getString(String fieldName) throws InvalidFieldException {
    validateField(fieldName, Collections.singleton(FieldType.Type.STRING));
    return (String) columns.get(fieldName);
  }

  @Nullable
  @Override
  public Float getFloat(String fieldName) throws InvalidFieldException {
    validateField(fieldName, Collections.singleton(FieldType.Type.FLOAT));
    return (Float) columns.get(fieldName);
  }

  @Nullable
  @Override
  public Double getDouble(String fieldName) throws InvalidFieldException {
    validateField(fieldName, ImmutableSet.of(FieldType.Type.FLOAT, FieldType.Type.DOUBLE));
    return (Double) columns.get(fieldName);
  }

  private void validateField(String fieldName, Set<FieldType.Type> validTypes) throws InvalidFieldException {
    FieldType.Type type = tableSchema.getType(fieldName);
    if (type == null || !validTypes.contains(type)) {
      throw new InvalidFieldException(tableSchema.getTableId(), fieldName);
    }
  }
}
