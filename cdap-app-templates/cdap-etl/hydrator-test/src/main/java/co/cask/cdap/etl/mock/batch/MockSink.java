/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.etl.mock.batch;

import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.cdap.test.DataSetManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Mock sink that writes records to a Table and has a utility method for getting all records written.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("Mock")
public class MockSink extends BatchSink<StructuredRecord, byte[], Put> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private static final byte[] SCHEMA_COL = Bytes.toBytes("s");
  private static final byte[] RECORD_COL = Bytes.toBytes("r");
  private final Config config;

  public MockSink(Config config) {
    this.config = config;
  }

  /**
   * Config for the sink.
   */
  public static class Config extends PluginConfig {
    @Macro
    private String tableName;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    if (!config.containsMacro("tableName")) {
      pipelineConfigurer.createDataset(config.tableName, Table.class);
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    if (!context.datasetExists(config.tableName)) {
      context.createDataset(config.tableName, "table", DatasetProperties.EMPTY);
    }
    context.addOutput(Output.ofDataset(config.tableName));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<byte[], Put>> emitter) throws Exception {
    byte[] ts = Bytes.toBytes(System.currentTimeMillis());
    byte[] rowkey = Bytes.concat(ts, Bytes.toBytes(UUID.randomUUID()));
    Put put = new Put(rowkey);
    put.add(SCHEMA_COL, input.getSchema().toString());
    put.add(RECORD_COL, StructuredRecordStringConverter.toJsonString(input));
    emitter.emit(new KeyValue<>(rowkey, put));
  }

  public static ETLPlugin getPlugin(String tableName) {
    Map<String, String> properties = new HashMap<>();
    properties.put("tableName", tableName);
    return new ETLPlugin("Mock", BatchSink.PLUGIN_TYPE, properties, null);
  }

  /**
   * Used to read the records written by this sink.
   *
   * @param tableManager dataset manager used to get the sink dataset to read from
   */
  public static List<StructuredRecord> readOutput(DataSetManager<Table> tableManager) throws Exception {
    tableManager.flush();
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

  /**
   * Clear any records written to this sink.
   *
   * @param tableManager dataset manager used to get the sink dataset
   */
  public static void clear(DataSetManager<Table> tableManager) {
    tableManager.flush();
    Table table = tableManager.get();

    try (Scanner scanner = table.scan(null, null)) {
      Row row;
      while ((row = scanner.next()) != null) {
        table.delete(row.getRow());
      }
    }
    tableManager.flush();
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("tableName", new PluginPropertyField("tableName", "", "string", true, true));
    return new PluginClass(BatchSink.PLUGIN_TYPE, "Mock", "", MockSink.class.getName(), "config", properties);
  }
}
