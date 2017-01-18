/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.ProjectInfo;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.filesystem.Location;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Builds alter table statements for Hive. Table DDL we support is of the form:
 *
 * ALTER TABLE table_name SET TBLPROPERTIES (property_name=property_value, ...);
 * ALTER TABLE table_name SET LOCATION hdfs_path;
 * ALTER TABLE table_name REPLACE COLUMNS (col_name data_type, ...)
 * ALTER TABLE table_name SET SERDEPROPERTIES ('field.delim' = '|')
 * ALTER TABLE table_name SET SERDE classname WITH SERDEPROPERTIES (...)
 * ALTER TABLE table_name SET FILEFORMAT INPUTFORMAT 'classname' OUTPUTFORMAT 'classname'
 *
 * We only support a subset of what Hive can do. For example, there is no support for SKEWED BY or CLUSTERED BY.
 */
public class AlterStatementBuilder {
  private final String name;
  private final String databaseName;
  private final String tableName;
  private boolean escapeColumns;

  public AlterStatementBuilder(String name, @Nullable String databaseName, String tableName, boolean escapeColumns) {
    this.name = name;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.escapeColumns = escapeColumns;
  }

  /**
   * Set the location of the Hive table.
   * For example:
   * ALTER TABLE mytable SET LOCATION '/my/path';
   */
  public String buildWithLocation(Location location) {
    return startBuild()
      .append("SET LOCATION '")
      .append(location.toURI().toString())
      .append("'")
      .toString();
  }

  /**
   * Set table properties. CDAP name and version must not be in the given properties, as they are added by the builder.
   */
  public String buildWithTableProperties(Map<String, String> tableProperties) {
    StringBuilder builder = startBuild()
      .append("SET TBLPROPERTIES ");
    appendMap(builder, addRequiredTableProperties(tableProperties));
    return builder.toString();
  }

  /**
   * Set the hive schema for the table. Should be of the form "column_name column_type, ...".
   */
  public String buildWithSchema(Schema schema) throws UnsupportedTypeException {
    return buildWithSchemaInternal(new SchemaConverter(escapeColumns).toHiveSchema(schema));
  }

  /**
   * Set the hive schema for the table. Should be of the form "column_name column_type, ...".
   * For example:
   * ALTER TABLE mytable REPLACE COLUMNS (a int, b float);
   */
  public String buildWithSchema(String hiveSchema) {
    return buildWithSchemaInternal("(" + hiveSchema + ")");
  }

  private String buildWithSchemaInternal(String hiveSchemaWithParentheses) {
    return startBuild()
      .append("REPLACE COLUMNS ")
      .append(hiveSchemaWithParentheses)
      .toString();

  }

  /**
   * Sets a native Hive file format for the table.
   * For example:
   * ALTER TABLE mytable SET FILEFORMAT avro;
   */
  public String buildWithFileFormat(String fileFormat) {
    return startBuild()
      .append("SET FILEFORMAT ")
      .append(fileFormat)
      .toString();
  }

  /**
   * Changes the delimiter for the row format. Note that there is no way to directly
   * change the row format in the ALTER TABLE syntax. Instead we have to set the
   * Serde property that controls the field delimiter.
   * See http://osdir.com/ml/hive-user-hadoop-apache/2009-12/msg00109.html.
   * For Example:
   * ALTER TABLE mytable SET SERDEPROPERTIES ('mapkey.delim' = '|');
   */
  public String buildWithDelimiter(String delimiter) {
    StringBuilder builder = startBuild()
      .append("SET SERDEPROPERTIES ");
    appendMap(builder, ImmutableMap.of("field.delim", delimiter == null ? "\\001" : delimiter));
    return builder.toString();
  }

  public String buildWithFormats(String inputFormat, String outputFormat, String serde) {
    return startBuild()
      .append("SET FILEFORMAT INPUTFORMAT '")
      .append(inputFormat)
      .append("' OUTPUTFORMAT '")
      .append(outputFormat)
      .append("' SERDE '")
      .append(serde)
      .append("'")
      .toString();
  }

  // required properties for every CDAP Hive table
  private Map<String, String> addRequiredTableProperties(Map<String, String> map) {
    return ImmutableMap.<String, String>builder().putAll(map)
      .put(Constants.Explore.CDAP_NAME, name)
      .put(Constants.Explore.CDAP_VERSION, ProjectInfo.getVersion().toString())
      .build();
  }

  /**
   * Start the create statement: ALTER TABLE tableName ...
   */
  private StringBuilder startBuild() {
    if (databaseName == null) {
      return new StringBuilder().append("ALTER TABLE ").append(tableName).append(' ');
    } else {
      return new StringBuilder().append("ALTER TABLE ").append(databaseName).append('.').append(tableName).append(' ');
    }
  }

  // appends the contents of the map as ('key'='val', ...). Also escapes any single quotes in the map.
  private void appendMap(StringBuilder strBuilder, Map<String, String> map) {
    boolean first = true;
    for (Map.Entry<String, String> entry : map.entrySet()) {
      strBuilder.append(first ? "('" : ", '")
        .append(entry.getKey().replaceAll("'", "\\\\'"))
        .append("'='")
        .append(entry.getValue().replaceAll("'", "\\\\'"))
        .append("'");
      first = false;
    }
    strBuilder.append(")");
  }

}
