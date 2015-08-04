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

package co.cask.cdap.template.etl.common;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.batch.sink.TableSink;
import co.cask.cdap.template.etl.batch.source.TableSource;
import co.cask.cdap.template.etl.realtime.sink.RealtimeTableSink;

import javax.annotation.Nullable;

/**
 * {@link PluginConfig} for {@link TableSource}, {@link TableSink} and {@link RealtimeTableSink}
 */
public class TableConfig extends PluginConfig {
  @Name(Properties.Table.NAME)
  @Description("Name of the table. If the table does not already exist, one will be created.")
  private String name;

  @Name(Properties.Table.PROPERTY_SCHEMA)
  @Description("Optional schema of the table as a JSON Object. If the table does not already exist, one will be " +
    "created with this schema, which will allow the table to be explored through Hive.")
  @Nullable
  private String schemaStr;

  @Name(Properties.Table.PROPERTY_SCHEMA_ROW_FIELD)
  @Description("The name of the record field that should be used as the row key when writing to the table.")
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
