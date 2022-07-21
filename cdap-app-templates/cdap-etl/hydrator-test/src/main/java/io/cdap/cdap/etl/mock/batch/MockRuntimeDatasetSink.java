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
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.cdap.test.DataSetManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Mock sink that tests creation of datasets during runtime based on if dataset exists or not.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("MockRuntime")
public class MockRuntimeDatasetSink extends BatchSink<StructuredRecord, byte[], Put> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private static final byte[] SCHEMA_COL = Bytes.toBytes("s");
  private static final byte[] RECORD_COL = Bytes.toBytes("r");
  private final Config config;

  public MockRuntimeDatasetSink(Config config) {
    this.config = config;
  }

  /**
   * Config for the sink.
   */
  public static class Config extends PluginConfig {
    @Macro
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
  public void prepareRun(BatchSinkContext context) throws Exception {
    context.addOutput(Output.ofDataset(config.tableName));
    if (!context.datasetExists(config.runtimeDatasetName)) {
      context.createDataset(config.runtimeDatasetName, KeyValueTable.class.getName(), DatasetProperties.EMPTY);
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
  }

  public static ETLPlugin getPlugin(String tableName, String runtimeDatasetName) {
    Map<String, String> properties = new HashMap<>();
    properties.put("tableName", tableName);
    properties.put("runtimeDatasetName", runtimeDatasetName);
    return new ETLPlugin("MockRuntime", BatchSink.PLUGIN_TYPE, properties, null);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<byte[], Put>> emitter) throws Exception {
    byte[] rowkey = Bytes.toBytes(UUID.randomUUID());
    Put put = new Put(rowkey);
    put.add(SCHEMA_COL, input.getSchema().toString());
    put.add(RECORD_COL, StructuredRecordStringConverter.toJsonString(input));
    emitter.emit(new KeyValue<>(rowkey, put));
  }

  /**
   * Used to read the records written by this sink.
   *
   * @param tableManager dataset manager used to get the sink dataset to read from
   */
  public static List<StructuredRecord> readOutput(DataSetManager<Table> tableManager) throws Exception {
    Table table = tableManager.get();

    try (Scanner scanner = table.scan(null, null)) {
      List<StructuredRecord> records = new ArrayList<>();
      Row row;
      while ((row = scanner.next()) != null) {
        Schema schema = Schema.parseJson(row.getString(SCHEMA_COL));
        String recordStr = row.getString(RECORD_COL);
        records.add(StructuredRecordStringConverter.fromJsonString(recordStr, schema));
      }
      return records;
    }
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("tableName", new PluginPropertyField("tableName", "", "string", true, true));
    properties.put("runtimeDatasetName", new PluginPropertyField("runtimeDatasetName", "", "string", true, true));
    return PluginClass.builder().setName("MockRuntime").setType(BatchSink.PLUGIN_TYPE)
             .setDescription("").setClassName(MockRuntimeDatasetSink.class.getName()).setProperties(properties)
             .setConfigFieldName("config").build();
  }
}
