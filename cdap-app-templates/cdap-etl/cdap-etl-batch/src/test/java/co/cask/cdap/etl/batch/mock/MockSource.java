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

package co.cask.cdap.etl.batch.mock;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.cdap.test.DataSetManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Mock source that can be used to write a list of records in a Table and reads them out in a pipeline run.
 * Can optionally add a 'runtime' column of type long to every record that contains the logical start time of the run.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("Mock")
public class MockSource extends BatchSource<byte[], Row, StructuredRecord> {
  private static final byte[] SCHEMA_COL = Bytes.toBytes("s");
  private static final byte[] RECORD_COL = Bytes.toBytes("r");
  private final Config config;
  private long runtime;

  public MockSource(Config config) {
    this.config = config;
  }

  public static class Config extends PluginConfig {
    private String tableName;

    @Nullable
    private Boolean addRuntime;

    public Config() {
      this.addRuntime = false;
      this.tableName = "";
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    pipelineConfigurer.createDataset(config.tableName, Table.class);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    runtime = context.getLogicalStartTime();
  }

  @Override
  public void transform(KeyValue<byte[], Row> input, Emitter<StructuredRecord> emitter) throws Exception {
    Schema schema = Schema.parseJson(input.getValue().getString(SCHEMA_COL));
    String recordStr = input.getValue().getString(RECORD_COL);
    StructuredRecord record = StructuredRecordStringConverter.fromJsonString(recordStr, schema);
    if (config.addRuntime) {
      record = addRuntimeColumn(record);
    }

    emitter.emit(record);
  }

  private StructuredRecord addRuntimeColumn(StructuredRecord record) {
    List<Schema.Field> fields = new ArrayList<>();
    fields.addAll(record.getSchema().getFields());
    fields.add(Schema.Field.of("runtime", Schema.of(Schema.Type.LONG)));
    Schema runtimeSchema = Schema.recordOf(record.getSchema().getRecordName(), fields);
    StructuredRecord.Builder builder = StructuredRecord.builder(runtimeSchema).set("runtime", runtime);
    for (Schema.Field field : record.getSchema().getFields()) {
      String fieldName = field.getName();
      builder.set(fieldName, record.get(fieldName));
    }
    return builder.build();
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    context.setInput(config.tableName);
  }

  public static co.cask.cdap.etl.common.Plugin getPlugin(String tableName) {
    return getPlugin(tableName, false);
  }

  public static co.cask.cdap.etl.common.Plugin getPlugin(String tableName, boolean addRuntime) {
    Map<String, String> properties = new HashMap<>();
    properties.put("tableName", tableName);
    properties.put("addRuntime", String.valueOf(addRuntime));
    return new co.cask.cdap.etl.common.Plugin("Mock", properties);
  }

  /**
   * Used to write the input records for the pipeline run. Should be called after the pipeline has been created.
   *
   * @param tableManager dataset manager used to write to the source dataset
   * @param records records that should be the input for the pipeline
   */
  public static void writeInput(DataSetManager<Table> tableManager, List<StructuredRecord> records) throws Exception {
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

}
