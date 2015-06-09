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

package co.cask.cdap.template.etl.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.template.etl.common.RecordPutTransformer;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * CDAP Table Dataset Batch Sink.
 */
@Plugin(type = "sink")
@Name("Table")
@Description("CDAP Table Dataset Batch Sink")
public class TableSink extends BatchWritableSink<StructuredRecord, byte[], Put> {
  private static final String NAME_DESC = "Name of the table dataset. If it does not already exist, one will be " +
    "created.";
  private static final String PROPERTY_SCHEMA_DESC = "Optional schema of the table as a JSON Object. If the table " +
    "does not already exist, one will be created with this schema, which will allow the table to be explored " +
    "through Hive.\"";
  private static final String PROPERTY_SCHEMA_ROW_FIELD_DESC = "The name of the record field that should be used as " +
    "the row key when writing to the table.";

  private RecordPutTransformer recordPutTransformer;

  /**
   * Config class for TableSink
   */
  public static class TableConfig extends PluginConfig {
    @Description(NAME_DESC)
    private String name;

    @Name(Properties.Table.PROPERTY_SCHEMA)
    @Description(PROPERTY_SCHEMA_DESC)
    @Nullable
    String schemaStr;

    @Name(Properties.Table.PROPERTY_SCHEMA_ROW_FIELD)
    @Description(PROPERTY_SCHEMA_ROW_FIELD_DESC)
    String rowField;

    public TableConfig(String name, String schemaStr, String rowField) {
      this.name = name;
      this.schemaStr = schemaStr;
      this.rowField = rowField;
    }
  }

  private final TableConfig tableConfig;

  public TableSink(TableConfig tableConfig) {
    this.tableConfig = tableConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableConfig.rowField), "Row field must be given as a property.");
  }

  @Override
  public void initialize(BatchSinkContext context) throws Exception {
    super.initialize(context);
    recordPutTransformer = new RecordPutTransformer(tableConfig.rowField);
  }

  @Override
  protected Map<String, String> getProperties() {
    Map<String, String> properties = Maps.newHashMap(tableConfig.getProperties().getProperties());
    properties.put(Properties.BatchReadableWritable.NAME, tableConfig.name);
    properties.put(Properties.BatchReadableWritable.TYPE, Table.class.getName());
    return properties;
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<byte[], Put>> emitter) throws Exception {
    Put put = recordPutTransformer.toPut(input);
    emitter.emit(new KeyValue<>(put.getRow(), put));
  }
}
