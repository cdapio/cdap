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

package co.cask.cdap.template.etl.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.batch.BatchSourceContext;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.template.etl.common.RowRecordTransformer;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * CDAP Table Dataset Batch Source.
 */
@Plugin(type = "source")
@Name("Table")
@Description("CDAP Table Dataset Batch Source")
public class TableSource extends BatchReadableSource<byte[], Row, StructuredRecord> {
  private RowRecordTransformer rowRecordTransformer;

  private static final String NAME_DESC = "Table name. If the table does not already exist, it will be created";
  private static final String PROPERTY_SCHEMA_DESC = "Schema of records read from the Table. Row columns map to " +
    "record fields. For example, if the schema contains a field named 'user' of type string, " +
    "the value of that field will be taken from the value stored in the 'user' column. " +
    "Only simple types are allowed (boolean, int, long, float, double, bytes, string).";
  private static final String PROPERTY_SCHEMA_ROW_FIELD_DESC = "Optional field name indicating that the field " +
    "value should come from the row key instead of a row column. The field name specified must be present in the " +
    "schema, and must not be nullable.";

  /**
   * Config class for TableSource
   */
  public static class TableConfig extends PluginConfig {
    @Description(NAME_DESC)
    String name;

    @Name(Properties.Table.PROPERTY_SCHEMA)
    @Description(PROPERTY_SCHEMA_DESC)
    String schemaStr;

    @Name(Properties.Table.PROPERTY_SCHEMA_ROW_FIELD)
    @Description(PROPERTY_SCHEMA_ROW_FIELD_DESC)
    @Nullable
    String rowField;

    public TableConfig(String name, String schemaStr, String rowField) {
      this.name = name;
      this.schemaStr = schemaStr;
      this.rowField = rowField;
    }
  }

  private final TableConfig tableConfig;

  public TableSource(TableConfig tableConfig) {
    this.tableConfig = tableConfig;
  }

  @Override
  protected Map<String, String> getProperties() {
    Map<String, String> properties = Maps.newHashMap(tableConfig.getProperties().getProperties());
    properties.put(Properties.BatchReadableWritable.NAME, tableConfig.name);
    properties.put(Properties.BatchReadableWritable.TYPE, Table.class.getName());
    return properties;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableConfig.schemaStr), "Schema must be specified.");
  }

  @Override
  public void initialize(BatchSourceContext context) throws Exception {
    super.initialize(context);
    Schema schema = Schema.parseJson(tableConfig.schemaStr);
    rowRecordTransformer = new RowRecordTransformer(schema, tableConfig.rowField);
  }

  @Override
  public void transform(KeyValue<byte[], Row> input, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(rowRecordTransformer.toRecord(input.getValue()));
  }
}
