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
import co.cask.cdap.template.etl.realtime.sink.RealtimeTableSink;

import javax.annotation.Nullable;

/**
 * {@link PluginConfig} for {@link TableSink} and {@link RealtimeTableSink}
 */
public class TableSinkConfig extends TableConfig {

  @Name(Properties.Table.CASE_SENSITIVE_ROW_FIELD)
  @Description("Whether the schema.row.field is case sensitive, defaults to true.")
  @Nullable
  private Boolean rowFieldCaseSensitive;

  public TableSinkConfig() {
    this.rowFieldCaseSensitive = true;
  }

  public Boolean isRowFieldCaseInsensitive() {
    return rowFieldCaseSensitive;
  }
}
