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

package co.cask.cdap.spi.data.table;

import co.cask.cdap.spi.data.table.field.FieldType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Class for the table schema, which provides convenient way to fetch for fields, primary key and index.
 */
public class StructuredTableSchema {
  private final StructuredTableId tableId;
  private final Map<String, FieldType.Type> fields;
  // primary keys have to be ordered as defined in the table schema
  private final List<String> primaryKeys;
  private final Set<String> indexes;

  public StructuredTableSchema(StructuredTableSpecification spec) {
    this.tableId = spec.getTableId();
    this.fields = Collections.unmodifiableMap(spec.getFieldTypes().stream().collect(
      Collectors.toMap(FieldType::getName, FieldType::getType)));
    this.primaryKeys = Collections.unmodifiableList(new ArrayList<>(spec.getPrimaryKeys()));
    this.indexes = Collections.unmodifiableSet(new HashSet<>(spec.getIndexes()));
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
}
