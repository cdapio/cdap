/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.explore.table;

import co.cask.cdap.api.dataset.lib.PartitionKey;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Builds alter table partition statements for Hive.
 */
public class AlterPartitionStatementBuilder {

  private final String databaseName;
  private final String tableName;
  private final PartitionKey partitionKey;
  private final boolean shouldEscapeColumns;

  public AlterPartitionStatementBuilder(@Nullable String databaseName, String tableName,
                                        PartitionKey partitionKey, boolean shouldEscapeColumns) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.partitionKey = partitionKey;
    this.shouldEscapeColumns = shouldEscapeColumns;
  }

  /**
   * Builds ADD PARTITION statement. For example:
   *   ALTER TABLE dataset_tpfs ADD PARTITION (year=2012) LOCATION '<uri>'
   */
  public String buildAddStatement(String fsPath) {
    return buildCommon()
      .append(" ADD PARTITION ")
      .append(generateHivePartitionKey(partitionKey))
      .append(" LOCATION '")
      .append(fsPath)
      .append("'")
      .toString();
  }

  /**
   * Builds DROP PARTITION statement. For example:
   *   ALTER TABLE dataset_tpfs DROP PARTITION (year=2012)
   */
  public String buildDropStatement() {
    return buildCommon()
      .append(" DROP PARTITION ")
      .append(generateHivePartitionKey(partitionKey))
      .toString();
  }

  /**
   * Builds PARTITION CONCATENATE statement. For example:
   *   ALTER TABLE dataset_tpfs PARTITION (year=2012) CONCATENATE
   */
  public String buildConcatenateStatement() {
    return buildCommon()
      .append(" PARTITION ")
      .append(generateHivePartitionKey(partitionKey))
      .append(" CONCATENATE")
      .toString();
  }

  private StringBuilder buildCommon() {
    StringBuilder str = new StringBuilder("ALTER TABLE ");
    if (databaseName != null) {
      str.append(databaseName).append(".");
    }
    str.append(tableName);
    return str;
  }

  private String generateHivePartitionKey(PartitionKey key) {
    StringBuilder builder = new StringBuilder("(");
    String sep = "";
    for (Map.Entry<String, ? extends Comparable> entry : key.getFields().entrySet()) {
      String fieldName = entry.getKey();
      Comparable fieldValue = entry.getValue();
      String quote = fieldValue instanceof String ? "'" : "";
      builder.append(sep);
      if (shouldEscapeColumns) {
        // a literal backtick(`) is just a double backtick(``)
        builder.append('`').append(fieldName.replace("`", "``")).append('`');
      } else {
        builder.append(fieldName);
      }
      builder.append("=").append(quote).append(fieldValue.toString()).append(quote);
      sep = ", ";
    }
    builder.append(")");
    return builder.toString();
  }
}
