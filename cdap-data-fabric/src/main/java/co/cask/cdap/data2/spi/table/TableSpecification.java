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

package co.cask.cdap.data2.spi.table;

import co.cask.cdap.data2.spi.InvalidFieldException;
import co.cask.cdap.data2.spi.table.field.FieldType;
import co.cask.cdap.data2.spi.table.field.Fields;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Defines the specification of a {@link co.cask.cdap.data2.spi.StructuredTable}.
 * The table specification contains:
 * <ul>
 *   <li>table Id - specifies the name of the table</li>
 *   <li>fields - the schema of the table, consists of the column names and their types</li>
 *   <li>primaryKeys - the primary key for each row</li>
 *   <li>indexes - the columns to index on. Only one column can be part of an index</li>
 * </ul>
 */
public final class TableSpecification {
  // Only alphanumeric and _ characters allowed in identifiers. Also, has to begin with an alphabet
  // This is to satisfy both SQL and HBase identifier name rules
  private static final Pattern IDENTIFIER_NAME_PATTERN = Pattern.compile("[a-zA-Z][a-zA-Z0-9_]*");

  private final TableId tableId;
  private final List<FieldType> fields;
  private final List<String> primaryKeys;
  private final List<String> indexes;

  /**
   * Use {@link Builder} to create instances.
   */
  private TableSpecification(TableId tableId, List<FieldType> fields, List<String> primaryKeys, List<String> indexes) {
    this.tableId = tableId;
    this.fields = Collections.unmodifiableList(fields);
    this.primaryKeys = Collections.unmodifiableList(primaryKeys);
    this.indexes = Collections.unmodifiableList(indexes);
  }

  public TableId getTableId() {
    return tableId;
  }

  public List<FieldType> getFields() {
    return fields;
  }

  public List<String> getPrimaryKeys() {
    return primaryKeys;
  }

  public List<String> getIndexes() {
    return indexes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableSpecification that = (TableSpecification) o;
    return Objects.equals(tableId, that.tableId) &&
      Objects.equals(fields, that.fields) &&
      Objects.equals(primaryKeys, that.primaryKeys) &&
      Objects.equals(indexes, that.indexes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableId, fields, primaryKeys, indexes);
  }

  @Override
  public String toString() {
    return "TableSpecification{" +
      "tableId='" + tableId + '\'' +
      ", fields=" + fields +
      ", primaryKeys=" + primaryKeys +
      ", indexes=" + indexes +
      '}';
  }

  public static final class Builder {
    private TableId tableId;
    private FieldType[] fieldTypes;
    private String[] primaryKeys;
    private String[] indexes;

    public Builder withId(TableId id) {
      this.tableId = id;
      return this;
    }

    public Builder withFields(FieldType ...fieldTypes) {
      this.fieldTypes = fieldTypes;
      return this;
    }

    public Builder withPrimaryKeys(String ...primaryKeys) {
      this.primaryKeys = primaryKeys;
      return this;
    }

    public Builder withIndexes(String ...indexes) {
      this.indexes = indexes;
      return this;
    }

    public TableSpecification build() {
      validate();
      return new TableSpecification(tableId, Arrays.asList(fieldTypes), Arrays.asList(primaryKeys),
                                    Arrays.asList(indexes));
    }

    private void validate() {
      if (tableId == null) {
        throw new IllegalArgumentException("TableId cannot be empty");
      }

      // Validate the table name is made up of valid characters
      if (!IDENTIFIER_NAME_PATTERN.matcher(tableId.getName()).matches()) {
        throw new IllegalArgumentException(
          String.format(
            "Invalid table name %s. Only alphanumeric and _ characters allowed, and should begin with an alphabet",
            tableId.getName()));
      }

      if (fieldTypes == null || fieldTypes.length == 0) {
        throw new IllegalArgumentException("No fields specified for the table " + tableId);
      }

      if (primaryKeys == null || primaryKeys.length == 0) {
        throw new IllegalArgumentException("No primary keys specified for the table " + tableId);
      }

      // A table may not have indexes
      if (indexes == null) {
        indexes = new String[0];
      }

      // Validate the field names are made up of valid characters
      for (FieldType fieldType : fieldTypes) {
        if (!IDENTIFIER_NAME_PATTERN.matcher(fieldType.getName()).matches()) {
          throw new IllegalArgumentException(
            String.format(
              "Invalid field name %s. Only alphanumeric and _ characters allowed, and should begin with an alphabet",
              fieldType.getName()));
        }
      }

      // Validate that the primary key is part of fields defined and of valid type
      Map<String, FieldType.Type> typeMap =
        Arrays.stream(fieldTypes).collect(Collectors.toMap(FieldType::getName, FieldType::getType));
      for (String primaryKey : primaryKeys) {
        FieldType.Type type = typeMap.get(primaryKey);
        if (type == null) {
          throw new InvalidFieldException(tableId, primaryKey);
        }
        if (!Fields.isPrimaryKeyType(type)) {
          throw new InvalidFieldException(tableId, primaryKey);
        }
      }

      // Validate that the indexes are part of the fields defined
      for (String index : indexes) {
        FieldType.Type type = typeMap.get(index);
        if (type == null) {
          throw new InvalidFieldException(tableId, index);
        }
      }
    }
  }
}
