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

package io.cdap.cdap.etl.mock.batch;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataException;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.data2.metadata.writer.MetadataOperation;
import io.cdap.cdap.data2.metadata.writer.MetadataOperationTypeAdapter;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.cdap.test.DataSetManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Mock source that can be used to write a list of records in a Table and reads them out in a pipeline run.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(MockSource.NAME)
public class MockSource extends BatchSource<byte[], Row, StructuredRecord> {
  public static final String NAME = "Mock";

  private static final Logger LOG = LoggerFactory.getLogger(MockSource.class);

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(MetadataOperation.class, new MetadataOperationTypeAdapter())
    .create();
  private static final Type SET_METADATA_OPERATION_TYPE = new TypeToken<Set<MetadataOperation>>() { }.getType();
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private static final byte[] SCHEMA_COL = Bytes.toBytes("s");
  private static final byte[] RECORD_COL = Bytes.toBytes("r");
  private final Config config;

  public MockSource(Config config) {
    this.config = config;
  }

  /**
   * Config for the source.
   */
  public static class Config extends PluginConfig {
    private String tableName;

    @Nullable
    private String schema;

    @Nullable
    private String metadataOperations;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    pipelineConfigurer.createDataset(config.tableName, Table.class);
    if (config.schema != null) {
      try {
        pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.parseJson(config.schema));
      } catch (IOException e) {
        throw new IllegalArgumentException("Could not parse schema " + config.schema, e);
      }
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    if (config.schema != null) {
      // should never happen, just done to test App correctness in unit tests
      Schema outputSchema = Schema.parseJson(config.schema);
      if (!outputSchema.equals(context.getOutputSchema())) {
        throw new IllegalStateException("Output schema does not match what was set at configure time.");
      }
    }
  }

  @Override
  public void transform(KeyValue<byte[], Row> input, Emitter<StructuredRecord> emitter) throws Exception {
    Schema schema = Schema.parseJson(input.getValue().getString(SCHEMA_COL));
    String recordStr = input.getValue().getString(RECORD_COL);
    emitter.emit(StructuredRecordStringConverter.fromJsonString(recordStr, schema));
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    context.setInput(Input.ofDataset(config.tableName));
    if (config.metadataOperations != null) {
      // if there are metadata operations to be performed then apply them
      processsMetadata(context);
    }
  }

  /**
   * Get the plugin config to be used in a pipeline config. If the source outputs records of the same schema,
   * {@link #getPlugin(String, Schema)} should be used instead, so that the source will set an output schema.
   *
   * @param tableName the table backing the mock source
   * @return the plugin config to be used in a pipeline config
   */
  public static ETLPlugin getPlugin(String tableName) {
    Map<String, String> properties = new HashMap<>();
    properties.put("tableName", tableName);
    return new ETLPlugin("Mock", BatchSource.PLUGIN_TYPE, properties, null);
  }

  /**
   * Get the plugin config to be used in a pipeline config. The source must only output records with the given schema.
   *
   * @param tableName the table backing the mock source
   * @param schema the schema of records output by this source
   * @param operations {@link MetadataOperation} to be performed
   * @return the plugin config to be used in a pipeline config
   */
  public static ETLPlugin getPlugin(String tableName, Schema schema, Set<MetadataOperation> operations) {
    Map<String, String> properties = new HashMap<>();
    properties.put("tableName", tableName);
    properties.put("schema", schema.toString());
    properties.put("metadataOperations", GSON.toJson(operations));
    return new ETLPlugin("Mock", BatchSource.PLUGIN_TYPE, properties, null);
  }

  /**
   * Get the plugin config to be used in a pipeline config. The source must only output records with the given schema.
   *
   * @param tableName the table backing the mock source
   * @param schema the schema of records output by this source
   * @return the plugin config to be used in a pipeline config
   */
  public static ETLPlugin getPlugin(String tableName, Schema schema) {
    Map<String, String> properties = new HashMap<>();
    properties.put("tableName", tableName);
    properties.put("schema", schema.toString());
    return new ETLPlugin("Mock", BatchSource.PLUGIN_TYPE, properties, null);
  }

  /**
   * Used to write the input records for the pipeline run. Should be called after the pipeline has been created.
   *
   * @param tableManager dataset manager used to write to the source dataset
   * @param records records that should be the input for the pipeline
   */
  public static void writeInput(DataSetManager<Table> tableManager,
                                Iterable<StructuredRecord> records) throws Exception {
    writeInput(tableManager, null, records);
  }

  /**
   * Used to write the input record with specified row key for the pipeline run.
   * Should be called after the pipeline has been created.
   *
   * @param tableManager dataset manager used to write to the source dataset
   * @param rowKey the row key of the table
   * @param record record that should be the input for the pipeline
   */
  public static void writeInput(DataSetManager<Table> tableManager, String rowKey,
                                StructuredRecord record) throws Exception {
    writeInput(tableManager, rowKey, ImmutableList.of(record));
  }

  private static void writeInput(DataSetManager<Table> tableManager, @Nullable String rowKey,
                                 Iterable<StructuredRecord> records) throws Exception {
    tableManager.flush();
    Table table = tableManager.get();
    // write each record as a separate row, with the serialized record as one column and schema as another
    // each rowkey will be a UUID.
    for (StructuredRecord record : records) {
      byte[] row = rowKey == null ? Bytes.toBytes(UUID.randomUUID()) : Bytes.toBytes(rowKey);
      table.put(row, SCHEMA_COL, Bytes.toBytes(record.getSchema().toString()));
      table.put(row, RECORD_COL, Bytes.toBytes(StructuredRecordStringConverter.toJsonString(record)));
    }
    tableManager.flush();
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("tableName", new PluginPropertyField("tableName", "", "string", true, false));
    properties.put("schema", new PluginPropertyField("schema", "", "string", false, false));
    properties.put("metadataOperations", new PluginPropertyField("metadataOperations", "", "string", false, false));
    return new PluginClass(BatchSource.PLUGIN_TYPE, "Mock", "", MockSource.class.getName(), "config", properties);
  }

  /**
   * Processes metadata operations
   */
  private void processsMetadata(BatchSourceContext context) throws MetadataException {
    MetadataEntity metadataEntity = MetadataEntity.ofDataset(context.getNamespace(), config.tableName);
    Map<MetadataScope, Metadata> currentMetadata = context.getMetadata(metadataEntity);
    Set<MetadataOperation> operations = GSON.fromJson(config.metadataOperations, SET_METADATA_OPERATION_TYPE);
    // must be to fetch metadata and there should be system metadata
    if (currentMetadata.get(MetadataScope.SYSTEM).getProperties().isEmpty() ||
      currentMetadata.get(MetadataScope.SYSTEM).getProperties().isEmpty()) {
      throw new IllegalArgumentException(String.format("System properties or tags for '%s' is empty. " +
                                                 "Expected to have system metadata.", metadataEntity));
    }
    LOG.trace("Metadata operations {} will be applied. Current Metadata Record is {}", operations, currentMetadata);
    // noinspection ConstantConditions
    for (MetadataOperation curOperation : operations) {
      switch (curOperation.getType()) {
        case PUT:
          // noinspection ConstantConditions
          context.addTags(curOperation.getEntity(), ((MetadataOperation.Put) curOperation).getTags());
          context.addProperties(curOperation.getEntity(), ((MetadataOperation.Put) curOperation).getProperties());
          break;
        case DELETE:
          // noinspection ConstantConditions
          context.removeTags(curOperation.getEntity(),
                             ((MetadataOperation.Delete) curOperation).getTags().toArray(new String[0]));
          context.removeProperties(curOperation.getEntity(),
                                   ((MetadataOperation.Delete) curOperation).getProperties().toArray(new String[0]));
          break;
        case DELETE_ALL:
          context.removeMetadata(curOperation.getEntity());
          break;
        case DELETE_ALL_TAGS:
          context.removeTags(curOperation.getEntity());
          break;
        case DELETE_ALL_PROPERTIES:
          context.removeProperties(curOperation.getEntity());
          break;
        default:
          throw new IllegalArgumentException(String.format("Invalid metadata operation '%s'", curOperation.getType()));
      }
    }
  }
}
