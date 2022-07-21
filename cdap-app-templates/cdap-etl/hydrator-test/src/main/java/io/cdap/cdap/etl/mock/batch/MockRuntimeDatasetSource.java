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

package io.cdap.cdap.etl.mock.batch;

import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.cdap.test.DataSetManager;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Mock source that tests creation of datasets during runtime based on if dataset exists or not.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("MockRuntime")
public class MockRuntimeDatasetSource extends BatchSource<byte[], Row, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private static final byte[] SCHEMA_COL = Bytes.toBytes("s");
  private static final byte[] RECORD_COL = Bytes.toBytes("r");
  private final Config config;

  public MockRuntimeDatasetSource(Config config) {
    this.config = config;
  }

  /**
   * Config for the source.
   */
  public static class Config extends PluginConfig {
    private String tableName;

    @Macro
    private String runtimeDatasetName;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    pipelineConfigurer.createDataset(config.tableName, Table.class);
    if (!config.containsMacro("runtimeDatasetName")) {
      pipelineConfigurer.createDataset(config.runtimeDatasetName, KeyValueTable.class.getName(),
                                       DatasetProperties.EMPTY);
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    context.setInput(Input.ofDataset(config.tableName));
    if (!context.datasetExists(config.runtimeDatasetName)) {
      context.createDataset(config.runtimeDatasetName, KeyValueTable.class.getName(), DatasetProperties.EMPTY);
    }
  }

  @Override
  public void transform(KeyValue<byte[], Row> input, Emitter<StructuredRecord> emitter) throws Exception {
    Schema schema = Schema.parseJson(input.getValue().getString(SCHEMA_COL));
    String recordStr = input.getValue().getString(RECORD_COL);
    emitter.emit(StructuredRecordStringConverter.fromJsonString(recordStr, schema));
  }

  public static ETLPlugin getPlugin(String tableName, String runtimeDatasetName) {
    Map<String, String> properties = new HashMap<>();
    properties.put("tableName", tableName);
    properties.put("runtimeDatasetName", runtimeDatasetName);
    return new ETLPlugin("MockRuntime", BatchSource.PLUGIN_TYPE, properties, null);
  }

  /**
   * Used to write the input records for the pipeline run. Should be called after the pipeline has been created.
   *
   * @param tableManager dataset manager used to write to the source dataset
   * @param records records that should be the input for the pipeline
   */
  public static void writeInput(DataSetManager<Table> tableManager,
                                Iterable<StructuredRecord> records) throws Exception {
    tableManager.flush();
    Table table = tableManager.get();
    // write each record as a separate row, with the serialized record as one column and schema as another
    // each rowkey will be a UUID.
    for (StructuredRecord record : records) {
      byte[] row = Bytes.toBytes(UUID.randomUUID());
      table.put(row, SCHEMA_COL, Bytes.toBytes(record.getSchema().toString()));
      table.put(row, RECORD_COL, Bytes.toBytes(StructuredRecordStringConverter.toJsonString(record)));
    }
    tableManager.flush();
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("tableName", new PluginPropertyField("tableName", "", "string", true, false));
    properties.put("runtimeDatasetName", new PluginPropertyField("runtimeDatasetName", "", "string", true, true));
    return PluginClass.builder().setName("MockRuntime").setType(BatchSource.PLUGIN_TYPE)
             .setDescription("").setClassName(MockRuntimeDatasetSource.class.getName()).setProperties(properties)
             .setConfigFieldName("config").build();
  }
}
