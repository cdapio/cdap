/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Common methods to read and validate table properties.
 */
public class TableProperties {

  private static final byte[] DEFAULT_DATA_COLUMN_FAMILY = Bytes.toBytes("d");

  /**
   * Extract the schema, parsed as Json, from the properties.
   *
   * @throws IllegalArgumentException if the schema cannot be parsed.
   */
  @Nullable
  static Schema getSchema(Map<String, String> props) {
    String schemaString = props.get(Table.PROPERTY_SCHEMA);
    try {
      return schemaString == null ? null : Schema.parseJson(schemaString);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid schema: " + schemaString, e);
    }
  }

  /**
   * Extract the field name to be used for the row key.
   */
  @Nullable
  static String getRowFieldName(Map<String, String> props) {
    return props.get(Table.PROPERTY_SCHEMA_ROW_FIELD);
  }

  /**
   * Extract the conflict detection level.
   *
   * @throws IllegalArgumentException if the property value is not a valid conflict detection level.
   */
  @Nullable
  static ConflictDetection getConflictDetectionLevel(Map<String, String> props, ConflictDetection defaultLevel) {
    String value = props.get(Table.PROPERTY_CONFLICT_LEVEL);
    if (value == null) {
      return defaultLevel;
    }
    try {
      return ConflictDetection.valueOf(value);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid conflict detection level: " + value);
    }
  }

  /**
   * Returns whether or not the dataset defined in the given specification should enable read-less increments.
   * Defaults to false.
   */
  public static boolean supportsReadlessIncrements(Map<String, String> props) {
    return "true".equalsIgnoreCase(props.get(Table.PROPERTY_READLESS_INCREMENT));
  }

  /**
   * Returns whether or not the dataset defined in the given specification is transactional.
   * Defaults to true.
   */
  public static boolean isTransactional(Map<String, String> props) {
    return !"true".equalsIgnoreCase(props.get(Constants.Dataset.TABLE_TX_DISABLED));
  }

  /**
   * Returns the column family as being set in the given specification.
   * If it is not set, the {@link #DEFAULT_DATA_COLUMN_FAMILY} will be returned.
   */
  public static byte[] getColumnFamily(Map<String, String> props) {
    String columnFamily = props.get(Table.PROPERTY_COLUMN_FAMILY);
    return columnFamily == null ? DEFAULT_DATA_COLUMN_FAMILY : Bytes.toBytes(columnFamily);
  }

}
