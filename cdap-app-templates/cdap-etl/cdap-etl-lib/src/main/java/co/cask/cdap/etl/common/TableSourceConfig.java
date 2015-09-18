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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.batch.sink.TableSink;
import co.cask.cdap.etl.batch.source.TableSource;
import co.cask.cdap.etl.realtime.sink.RealtimeTableSink;

import javax.annotation.Nullable;

/**
 * {@link PluginConfig} for {@link TableSource}, {@link TableSink} and {@link RealtimeTableSink}
 */
public class TableSourceConfig extends PluginConfig {
  @Name(Properties.Table.NAME)
  @Description("Name of the table. If the table does not already exist, one will be created.")
  private String name;

  @Name(Properties.Table.PROPERTY_SCHEMA)
  @Description("Optional schema of the table as a JSON Object. If the table does not already exist, one will be " +
    "created with this schema, which will allow the table to be explored through Hive.")
  @Nullable
  private String schemaStr;

  @Name(Properties.Table.PROPERTY_SCHEMA_ROW_FIELD)
  @Description("Optional field name indicating that the field value should come from the row key instead of a " +
    "row column. The field name specified must be present in the schema, and must not be nullable.")
  @Nullable
  private String rowField;

  public String getName() {
    return name;
  }

  @Nullable
  public String getSchemaStr() {
    return schemaStr;
  }

  public String getRowField() {
    return rowField;
  }
}
