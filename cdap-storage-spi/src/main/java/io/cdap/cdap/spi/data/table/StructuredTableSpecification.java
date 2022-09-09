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

package io.cdap.cdap.spi.data.table;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.spi.data.InvalidFieldException;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.field.FieldType;
import io.cdap.cdap.spi.data.table.field.Fields;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Defines the specification of a {@link StructuredTable}.
 * The table specification contains:
 * <ul>
 *   <li>table Id - specifies the name of the table</li>
 *   <li>fields - the schema of the table, consists of the column names and their types</li>
 *   <li>primaryKeys - the primary key for each row</li>
 *   <li>indexes - the columns to index on. Only one column can be part of an index</li>
 * </ul>
 */
@Beta
public final class StructuredTableSpecification {
  // Only alphanumeric and _ characters allowed in identifiers. Also, has to begin with an alphabet
  // This is to satisfy both SQL and HBase identifier name rules
  private static final Pattern IDENTIFIER_NAME_PATTERN = Pattern.compile("[a-zA-Z][a-zA-Z0-9_]*");

  private final StructuredTableId tableId;
  private final List<FieldType> fieldTypes;
  private final List<String> primaryKeys;
  private final List<String> indexes;

  /**
   * Use {@link Builder} to create instances.
   */
  private StructuredTableSpecification(StructuredTableId tableId, List<FieldType> fieldTypes, List<String> primaryKeys,
                                       List<String> indexes) {
    this.tableId = tableId;
    this.fieldTypes = Collections.unmodifiableList(fieldTypes);
    this.primaryKeys = Collections.unmodifiableList(primaryKeys);
    this.indexes = Collections.unmodifiableList(indexes);
  }

  /**
   * @return the table id of this table specification
   */
  public StructuredTableId getTableId() {
    return tableId;
  }

  /**
   * @return the field types of the table
   */
  public List<FieldType> getFieldTypes() {
    return fieldTypes;
  }

  /**
   * @return the list of primary keys defined on the table
   */
  public List<String> getPrimaryKeys() {
    return primaryKeys;
  }

  /**
   * @return the list of indexes defined on the table
   */
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
    StructuredTableSpecification that = (StructuredTableSpecification) o;
    return Objects.equals(tableId, that.tableId) &&
      Objects.equals(fieldTypes, that.fieldTypes) &&
      Objects.equals(primaryKeys, that.primaryKeys) &&
      Objects.equals(indexes, that.indexes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableId, fieldTypes, primaryKeys, indexes);
  }

  @Override
  public String toString() {
    return "StructuredTableSpecification{" +
      "tableId='" + tableId + '\'' +
      ", fieldTypes=" + fieldTypes +
      ", primaryKeys=" + primaryKeys +
      ", indexes=" + indexes +
      '}';
  }

  /**
   * Builder used to create {@link StructuredTableSpecification}
   */
  public static final class Builder {
    private StructuredTableId tableId;
    private List<FieldType> fieldTypes;
    private List<String> primaryKeys;
    private List<String> indexes;

    /**
     * Create a builder that is initialized with all the information from an existing specification.
     */
    public Builder() {
      this.fieldTypes = new ArrayList<>();
      this.primaryKeys = new ArrayList<>();
      this.indexes = new ArrayList<>();
    }

    /**
     * Create a builder that is initialized with all the information from an existing specification.
     */
    public Builder(StructuredTableSpecification existing) {
      this.tableId = existing.getTableId();
      this.fieldTypes = new ArrayList<>(existing.getFieldTypes());
      this.primaryKeys = new ArrayList<>(existing.getPrimaryKeys());
      this.indexes = new ArrayList<>(existing.getIndexes());
    }

    /**
     * Set the table id. A table should have an id.
     *
     * @param id table id
     * @return Builder instance
     */
    public Builder withId(StructuredTableId id) {
      this.tableId = id;
      return this;
    }

    /**
     * Set the field types in the table schema. A table should have at least one field.
     *
     * @param fieldTypes list of field types
     * @return Builder instance
     */
    public Builder withFields(FieldType... fieldTypes) {
      this.fieldTypes = Arrays.asList(fieldTypes);
      return this;
    }

    /**
     * Set the fields that form the primary keys of the table. A table should have at least one primary key.
     * See {@link FieldType#PRIMARY_KEY_TYPES} for valid primary key field types.
     *
     * @param primaryKeys list of field names forming the primary keys
     * @return Builder instance
     */
    public Builder withPrimaryKeys(String... primaryKeys) {
      this.primaryKeys = Arrays.asList(primaryKeys);
      return this;
    }

    /**
     * Set the fields that need to be indexed in the table. A table need not define any indexes.
     * See {@link FieldType#INDEX_COLUMN_TYPES} for valid index field types.
     *
     * @param indexes list of field names for the index
     * @return Builder instance
     */
    public Builder withIndexes(String... indexes) {
      if (indexes != null) {
        this.indexes = Arrays.asList(indexes);
      }
      return this;
    }

    /**
     * Build the table specification
     *
     * @return the table specification
     */
    public StructuredTableSpecification build() throws InvalidFieldException {
      validate();
      return new StructuredTableSpecification(tableId, fieldTypes, primaryKeys, indexes);
    }

    private void validate() throws InvalidFieldException {
      if (tableId == null) {
        throw new IllegalArgumentException("StructuredTableId cannot be empty");
      }

      // Validate the table name is made up of valid characters
      if (!IDENTIFIER_NAME_PATTERN.matcher(tableId.getName()).matches()) {
        throw new IllegalArgumentException(
          String.format(
            "Invalid table name %s. Only alphanumeric and _ characters allowed, and should begin with an alphabet",
            tableId.getName()));
      }

      if (fieldTypes == null || fieldTypes.size() == 0) {
        throw new IllegalArgumentException("No fieldTypes specified for the table " + tableId);
      }

      if (primaryKeys == null || primaryKeys.size() == 0) {
        throw new IllegalArgumentException("No primary keys specified for the table " + tableId);
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
      Map<String, FieldType.Type> typeMap = fieldTypes.stream()
        .collect(Collectors.toMap(FieldType::getName, FieldType::getType));
      for (String primaryKey : primaryKeys) {
        FieldType.Type type = typeMap.get(primaryKey);
        if (type == null) {
          throw new InvalidFieldException(tableId, primaryKey);
        }
        if (!Fields.isPrimaryKeyType(type)) {
          throw new InvalidFieldException(
            tableId, primaryKey,
            String.format("has wrong type for a primary key. Valid types are: %s", FieldType.PRIMARY_KEY_TYPES));
        }
      }

      // Validate that the indexes are part of the fields defined and of valid type
      for (String index : indexes) {
        FieldType.Type type = typeMap.get(index);
        if (type == null) {
          throw new InvalidFieldException(tableId, index, "is not defined as an index column");
        }
        if (!Fields.isIndexColumnType(type)) {
          throw new InvalidFieldException(
            tableId, index,
            String.format("has wrong type for an index column. Valid types are: %s", FieldType.INDEX_COLUMN_TYPES));
        }
      }
    }
  }
}
