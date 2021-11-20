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

package io.cdap.cdap.spi.data.table;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.spi.data.table.field.FieldType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Class for the table schema, which provides convenient way to fetch for fields, primary key and index.
 */
@Beta
public class StructuredTableSchema {
  private final StructuredTableId tableId;
  private final Map<String, FieldType.Type> fields;
  // primary keys have to be ordered as defined in the table schema
  private final List<String> primaryKeys;
  private final Set<String> indexes;

  public StructuredTableSchema(StructuredTableSpecification spec) {
    this(spec.getTableId(), spec.getFieldTypes(), spec.getPrimaryKeys(), spec.getIndexes());
  }

  public StructuredTableSchema(StructuredTableId tableId, List<FieldType> fields,
                               List<String> primaryKeys, Collection<String> indexes) {
    this.tableId = tableId;
    this.fields = Collections.unmodifiableMap(fields.stream().collect(
      Collectors.toMap(FieldType::getName, FieldType::getType)));
    this.primaryKeys = Collections.unmodifiableList(new ArrayList<>(primaryKeys));
    this.indexes = Collections.unmodifiableSet(new HashSet<>(indexes));
  }

  public StructuredTableId getTableId() {
    return tableId;
  }

  public List<String> getPrimaryKeys() {
    return primaryKeys;
  }

  public Set<String> getIndexes() {
    return indexes;
  }

  /**
   * Check if the given field name is a column of the primary keys.
   *
   * @param fieldName the field name to be checked
   * @return true if this field name is a column of primary keys, false otherwise
   */
  public boolean isPrimaryKeyColumn(String fieldName) {
    return primaryKeys.contains(fieldName);
  }

  /**
   * Check if the given field name is an index column.
   *
   * @param fieldName the field name to be checked
   * @return true if this field name is an index column, false otherwise
   */
  public boolean isIndexColumn(String fieldName) {
    return indexes.contains(fieldName);
  }

  /**
   * Get the field type of the given field name.
   *
   * @param fieldName the field name
   * @return the field type of the fieldname, null if not present in schema
   */
  @Nullable
  public FieldType.Type getType(String fieldName) {
    return fields.get(fieldName);
  }

  public Set<String> getFieldNames() {
    return fields.keySet();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    StructuredTableSchema that = (StructuredTableSchema) other;
    return Objects.equals(tableId, that.tableId)
      && Objects.equals(fields, that.fields)
      && Objects.equals(primaryKeys, that.primaryKeys)
      && Objects.equals(indexes, that.indexes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableId, fields, primaryKeys, indexes);
  }
  
  /**
   * Checks if this schema is compatible with the given {@link StructuredTableSpecification}. They are compatible if
   *
   * <ol>
   *   <li>
   *     This schema contains all the fields in the specification.
   *   </li>
   *   <li>
   *     Each schema field has data type that can store the corresponding spec field data without losing precision.
   *   </li>
   *   <li>
   *     They have the same set of primary keys.
   *   </li>
   *   <li>
   *     They have the same set of indexes.
   *   </li>
   * </ol>
   *
   * @param spec the {@link StructuredTableSpecification} to check for compatibility
   * @return {@code true} if this schema is compatible with the given specification, otherwise return {@code false}
   */
  public boolean isCompatible(StructuredTableSpecification spec) {
    for (FieldType field : spec.getFieldTypes()) {
      FieldType.Type type = getType(field.getName());
      if (type == null || !type.isCompatible(field.getType())) {
        return false;
      }
    }

    return getPrimaryKeys().equals(spec.getPrimaryKeys())
      && getIndexes().equals(new HashSet<>(spec.getIndexes()));
  }
}
