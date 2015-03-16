/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.data2.dataset2.lib.partitioned.FieldTypes;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.twill.filesystem.Location;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Builds create table statements for Hive. Table DDL we support is of the form:
 *
 * CREATE EXTERNAL TABLE IF NOT EXISTS [db_name.]table_name
 *   [(col_name data_type, ...)]
 *   [COMMENT table_comment]
 *   [PARTITIONED BY (col_name data_type, ...)]
 * [
 *   [ROW FORMAT row_format]
 *   [STORED AS file_format]
 *     | STORED BY 'storage.handler.class.name' [WITH SERDEPROPERTIES (...)]
 * ]
 * [LOCATION hdfs_path]
 * [TBLPROPERTIES (property_name=property_value, ...)]
 *
 *
 * row_format
 *   : DELIMITED [FIELDS TERMINATED BY char [ESCAPED BY char]]
 *   | SERDE serde_name [WITH SERDEPROPERTIES (property_name=property_value, property_name=property_value, ...)]
 *
 * file_format:
 *   : SEQUENCEFILE
 *   | TEXTFILE    -- (Default, depending on hive.default.fileformat configuration)
 *   | RCFILE      -- (Note: Available in Hive 0.6.0 and later)
 *   | ORC         -- (Note: Available in Hive 0.11.0 and later)
 *   | PARQUET     -- (Note: Available in Hive 0.13.0 and later)
 *   | AVRO        -- (Note: Available in Hive 0.14.0 and later)
 *   | INPUTFORMAT input_format_classname OUTPUTFORMAT output_format_classname
 *
 * We only support a subset of what Hive can do. For example, there is no support for SKEWED BY or CLUSTERED BY.
 */
public class CreateStatementBuilder {
  private final String name;
  private final String hiveTableName;
  private String hiveSchema;
  private String location;
  private String tableComment;
  private String rowFormat;
  private Partitioning partitioning;
  private Map<String, String> tableProperties;

  public CreateStatementBuilder(String name, String hiveTableName) {
    this.name = name;
    this.hiveTableName = hiveTableName;
    this.tableProperties = addRequiredTableProperties(Maps.<String, String>newHashMap());
  }

  /**
   * Set the schema for the table. Throws an exception if it is not valid for Hive.
   */
  public CreateStatementBuilder setSchema(Schema schema) throws UnsupportedTypeException {
    this.hiveSchema = SchemaConverter.toHiveSchema(schema);
    return this;
  }

  /**
   * Set the hive schema for the table. Should be of the form "column_name column_type, ...".
   */
  public CreateStatementBuilder setSchema(String hiveSchema) {
    this.hiveSchema = "(" + hiveSchema + ")";
    return this;
  }

  /**
   * Set table properties. CDAP name and version must not be in the given properties, as they are added by the builder.
   */
  public CreateStatementBuilder setTableProperties(Map<String, String> tableProperties) {
    this.tableProperties = addRequiredTableProperties(tableProperties);
    return this;
  }

  /**
   * Set the location of the Hive table.
   */
  public CreateStatementBuilder setLocation(String location) {
    this.location = location;
    return this;
  }

  /**
   * Set the location of the Hive table.
   */
  public CreateStatementBuilder setLocation(Location location) {
    this.location = location.toURI().toString();
    return this;
  }

  /**
   * Set partitions of the Hive table.
   */
  public CreateStatementBuilder setPartitioning(Partitioning partitioning) {
    this.partitioning = partitioning;
    return this;
  }

  /**
   * Set a comment for the Hive table.
   */
  public CreateStatementBuilder setTableComment(String tableComment) {
    this.tableComment = tableComment;
    return this;
  }

  /**
   * Set the row format serde without properties.
   */
  public CreateStatementBuilder setRowFormatSerde(String rowFormatSerde) {
    return setRowFormatSerde(rowFormatSerde, null);
  }

  /**
   * Set the row format serde with properties. Corresponds to using:
   * ROW FORMAT SERDE serde_name [WITH SERDEPROPERTIES (property_name=property_value, ...)].
   */
  public CreateStatementBuilder setRowFormatSerde(String rowFormatSerde,
                                                  @Nullable Map<String, String> serdeProperties) {
    Preconditions.checkArgument(rowFormat == null, "row format can only be set once.");
    StringBuilder strBuilder = new StringBuilder()
      .append("SERDE '")
      .append(rowFormatSerde)
      .append("'");
    if (serdeProperties != null && !serdeProperties.isEmpty()) {
      strBuilder.append(" WITH SERDEPROPERTIES ");
      appendMap(strBuilder, serdeProperties);
    }
    this.rowFormat = strBuilder.toString();
    return this;
  }

  /**
   * Set the row format using delimited by. Corresponds to using:
   * ROW FORMAT DELIMITED [FIELDS TERMINATED BY char [ESCAPED BY char]]
   * The escapedBy char can only be given if terminatedBy is not null.
   */
  public CreateStatementBuilder setRowFormatDelimited(@Nullable String terminatedBy, @Nullable String escapedBy) {
    Preconditions.checkArgument(rowFormat == null, "row format can only be set once.");
    StringBuilder strBuilder = new StringBuilder()
      .append("DELIMITED");

    if (terminatedBy != null && !terminatedBy.isEmpty()) {
      strBuilder.append(" FIELDS TERMINATED BY '")
        .append(terminatedBy)
        .append("'");
      if (escapedBy != null && !escapedBy.isEmpty()) {
        strBuilder.append(" ESCAPED BY '")
          .append(escapedBy)
          .append("'");
      }
    }
    this.rowFormat = strBuilder.toString();
    return this;
  }

  /**
   * Builds a create statement for a custom (non-native) Hive table. This means it uses a storage handler
   * and has optional serde properties. For example:
   *
   * CREATE EXTERNAL TABLE IF NOT EXISTS nn
   *   STORED BY 'co.cask.cdap.hive.datasets.DatasetStorageHandler'
   *     WITH SERDEPROPERTIES ('cdap.name' = 'nn', 'cdap.namespace' = 'default')
   * LOCATION '<uri>'
   * TBLPROPERTIES ( ... )
   */
  public String buildWithStorageHandler(String storageHandler, @Nullable Map<String, String> serdeProperties) {
    StringBuilder strBuilder = startBuild()
      .append(" STORED BY '")
      .append(storageHandler)
      .append("'");

    if (serdeProperties != null && !serdeProperties.isEmpty()) {
      strBuilder.append(" WITH SERDEPROPERTIES ");
      appendMap(strBuilder, serdeProperties);
    }

    return finishBuild(strBuilder);
  }

  /**
   * Builds a create statement for a native Hive table which is stored as a native Hive file format.
   * For example:
   * CREATE EXTERNAL TABLE IF NOT EXISTS nn
   *   [ PARTITIONED BY (field type, ...) ]
   *   ROW FORMAT SERDE '<serde class>'
   *   STORED AS 'avro'
   *   LOCATION '<uri>'
   *   TBLPROPERTIES ('avro.schema.literal'='...');
   */
  public String buildWithFileFormat(String fileFormat) {
    StringBuilder strBuilder = startBuild()
      .append(" STORED AS ")
      .append(fileFormat);

    return finishBuild(strBuilder);
  }

  /**
   * Builds a create statement for a native Hive table which is stored as a file format that uses an input format
   * and output format.
   * For example:
   * CREATE EXTERNAL TABLE IF NOT EXISTS nn
   *   [ PARTITIONED BY (field type, ...) ]
   *   ROW FORMAT SERDE '<serde class>'
   *   STORED AS INPUTFORMAT '<input format class>'
   *             OUTPUTFORMAT '<output format class>'
   *   LOCATION '<uri>'
   *   TBLPROPERTIES ('avro.schema.literal'='...');
   */
  public String buildWithFormats(String inputFormat, String outputFormat) {
    StringBuilder strBuilder = startBuild()
      .append(" STORED AS INPUTFORMAT '")
      .append(inputFormat)
      .append("' OUTPUTFORMAT '")
      .append(outputFormat)
      .append("'");

    return finishBuild(strBuilder);
  }

  // required properties for every CDAP Hive table
  private Map<String, String> addRequiredTableProperties(Map<String, String> map) {
    return ImmutableMap.<String, String>builder().putAll(map)
      .put(Constants.Explore.CDAP_NAME, name)
      .put(Constants.Explore.CDAP_VERSION, ProjectInfo.getVersion().toString())
      .build();
  }

  /**
   * Start the create statement.
   * CREATE EXTERNAL TABLE IF NOT EXISTS [db_name.]table_name
   *   [(col_name data_type, ...)]
   *   [COMMENT table_comment]
   *   [PARTITIONED BY (col_name data_type, ...)]
   */
  private StringBuilder startBuild() {
    StringBuilder strBuilder = new StringBuilder()
      .append("CREATE EXTERNAL TABLE IF NOT EXISTS ")
      .append(hiveTableName);

    // yeah... schema is not always required.
    if (hiveSchema != null) {
      strBuilder.append(" ").append(hiveSchema);
    }

    if (tableComment != null && !tableComment.isEmpty()) {
      strBuilder.append(" COMMENT '")
        .append(tableComment)
        .append("'");
    }

    if (partitioning != null && !partitioning.getFields().isEmpty()) {
      strBuilder.append(" PARTITIONED BY (");
      for (Map.Entry<String, Partitioning.FieldType> entry : partitioning.getFields().entrySet()) {
        strBuilder.append(entry.getKey())
          .append(" ")
          .append(FieldTypes.toHiveType(entry.getValue()))
          .append(", ");
      }
      // remove trailing ", "
      strBuilder.deleteCharAt(strBuilder.length() - 1)
        .deleteCharAt(strBuilder.length() - 1)
        .append(")");
    }

    if (rowFormat != null) {
      strBuilder.append(" ROW FORMAT ").append(rowFormat);
    }
    return strBuilder;
  }

  /**
   * Finish the create statement:
   * ...
   * [LOCATION hdfs_path]
   * [TBLPROPERTIES (property_name=property_value, ...)]
   */
  private String finishBuild(StringBuilder strBuilder) {
    if (location != null && !location.isEmpty()) {
      strBuilder.append(" LOCATION '")
        .append(location)
        .append("'");
    }

    // table properties is never empty because of required cdap properties
    strBuilder.append(" TBLPROPERTIES ");
    appendMap(strBuilder, tableProperties);
    return strBuilder.toString();
  }

  // appends the contents of the map as ('key'='val', ...). Also escapes any single quotes in the map.
  private void appendMap(StringBuilder strBuilder, Map<String, String> map) {
    strBuilder.append("(");
    for (Map.Entry<String, String> entry : map.entrySet()) {
      strBuilder.append("'")
        .append(entry.getKey().replaceAll("'", "\\\\'"))
        .append("'='")
        .append(entry.getValue().replaceAll("'", "\\\\'"))
        .append("', ");
    }
    // remove trailing ", "
    strBuilder.deleteCharAt(strBuilder.length() - 1)
      .deleteCharAt(strBuilder.length() - 1)
      .append(")");
  }

}
