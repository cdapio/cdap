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

package co.cask.cdap.spi.data.table;

import co.cask.cdap.spi.data.table.field.FieldType;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 */
public class TableSchema {
  private final StructuredTableId tableId;
  private final Map<String, FieldType.Type> fields;
  // primary keys have to be ordered as defined in the table schema
  private final Set<String> primaryKeys;
  private final Set<String> indexes;

  public TableSchema(StructuredTableId tableId, Collection<FieldType> fields, Collection<String> primaryKeys,
              Collection<String> indexes) {
    this.tableId = tableId;
    this.fields = Collections.unmodifiableMap(fields.stream().collect(Collectors.toMap(FieldType::getName,
                                                                                       FieldType::getType)));
    this.primaryKeys = Collections.unmodifiableSet(new LinkedHashSet<>(primaryKeys));
    this.indexes = Collections.unmodifiableSet(new HashSet<>(indexes));
  }

  public TableSchema(StructuredTableSpecification spec) {
    this(spec.getTableId(), spec.getFieldTypes(), spec.getPrimaryKeys(), spec.getIndexes());
  }

  public StructuredTableId getTableId() {
    return tableId;
  }

  public boolean isKey(String fieldName) {
    return primaryKeys.contains(fieldName);
  }

  public boolean isIndex(String fieldName) {
    return indexes.contains(fieldName);
  }

  public FieldType.Type getType(String fieldName) {
    return fields.get(fieldName);
  }

  public Set<String> getPrimaryKeys() {
    return primaryKeys;
  }
}
