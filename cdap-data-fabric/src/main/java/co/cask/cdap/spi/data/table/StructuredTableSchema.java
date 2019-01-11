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

import co.cask.cdap.spi.data.InvalidFieldException;
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

  /**
   * Validate if the given keys are prefix or complete primary keys.
   *
   * @param keys the keys to validate
   * @param allowPrefix boolean to indicate whether the given collection keys can be a prefix of the primary keys
   * @throws InvalidFieldException if the given keys have extra key which is not in primary key, or are not in correct
   * order of the primary keys or are not complete keys if allowPrefix is set to false.
   */
  public void validatePrimaryKeys(List<String> keys, boolean allowPrefix) throws InvalidFieldException {
    if (keys.size() > primaryKeys.size()) {
      throw new InvalidFieldException(tableId, keys, String.format("Given keys %s contains more fields than the" +
                                                                     " primary keys %s", keys, primaryKeys));
    }

    if (!allowPrefix && keys.size() < primaryKeys.size()) {
      throw new InvalidFieldException(tableId, keys,
                                      String.format("Given keys %s do not contain all the primary keys %s", keys,
                                                    primaryKeys));
    }

    if (Collections.indexOfSubList(primaryKeys, keys) == -1) {
      throw new InvalidFieldException(tableId, keys, String.format("Given keys %s are not the prefix of " +
                                                                     "the primary keys %s", keys, primaryKeys));
    }
  }
}
