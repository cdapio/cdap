/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.mock.batch.aggregator;

import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchAggregatorContext;
import io.cdap.cdap.etl.api.batch.BatchReduceAggregator;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;

import java.util.HashMap;
import java.util.Map;

/**
 * Groups on a specific field and adds count field. Used to test that the right values are going to the
 * right groups, to test multiple group keys for the same value, and to test setting the group key class
 * at runtime, and to test setting a supported non-writable class.
 */
@Plugin(type = BatchReduceAggregator.PLUGIN_TYPE)
@Name(FieldCountReduceAggregator.NAME)
public class FieldCountReduceAggregator extends BatchReduceAggregator<Object, StructuredRecord, StructuredRecord> {
  public static final String NAME = "FieldCount Aggregator";
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private static final Schema REDUCE_SCHEMA =
    Schema.recordOf("record", Schema.Field.of("fc", Schema.of(Schema.Type.LONG)));
  private final Config config;
  private Schema schema;

  public FieldCountReduceAggregator(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    if (!config.containsMacro("fieldType") && !config.containsMacro("fieldName")) {
      stageConfigurer.setOutputSchema(config.getSchema());
    }
  }

  @Override
  public void prepareRun(BatchAggregatorContext context) throws Exception {
    if ("long".equalsIgnoreCase(config.fieldType)) {
      context.setGroupKeyClass(Long.class);
    } else {
      context.setGroupKeyClass(String.class);
    }
  }

  @Override
  public void groupBy(StructuredRecord input, Emitter<Object> emitter) throws Exception {
    if ("long".equalsIgnoreCase(config.fieldType)) {
      emitter.emit(input.get(config.fieldName));
      emitter.emit(0L);
    } else {
      emitter.emit(input.get(config.fieldName).toString());
      emitter.emit("all");
    }
  }

  @Override
  public StructuredRecord reduce(StructuredRecord value1, StructuredRecord value2) throws Exception {
    Long v1Ct = value1.get("fc");
    Long v2Ct = value2.get("fc");
    long count = (v1Ct == null ? 1L : v1Ct) + (v2Ct == null ? 1L : v2Ct);
    return StructuredRecord.builder(REDUCE_SCHEMA).set("fc", count).build();
  }

  @Override
  public void finalize(Object groupKey, StructuredRecord groupValue,
                       Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(StructuredRecord.builder(schema)
                   .set(config.fieldName, groupKey)
                   .set("ct", groupValue.get("fc") == null ? 1L : groupValue.get("fc"))
                   .build());
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    schema = config.getSchema();
    // should never happen, just done to test App correctness in unit tests
    if (context.getOutputSchema() != null && !schema.equals(context.getOutputSchema())) {
      throw new IllegalStateException("Output schema does not match what was set at configure time.");
    }
  }

  /**
   * Conf for the aggregator.
   */
  public static class Config extends PluginConfig {
    @Macro
    private final String fieldName;

    @Macro
    private final String fieldType;

    public Config() {
      this.fieldName = "field";
      this.fieldType = "string";
    }

    private Schema getSchema() {
      Schema.Field fieldSchema;
      if ("string".equalsIgnoreCase(fieldType)) {
        fieldSchema = Schema.Field.of(fieldName, Schema.of(Schema.Type.STRING));
      } else if ("long".equalsIgnoreCase(fieldType)) {
        fieldSchema = Schema.Field.of(fieldName, Schema.of(Schema.Type.LONG));
      } else {
        throw new IllegalArgumentException("Unsupported field type " + fieldType);
      }

      return Schema.recordOf(
        fieldName + ".count",
        fieldSchema,
        Schema.Field.of("ct", Schema.of(Schema.Type.LONG)));
    }
  }

  public static ETLPlugin getPlugin(String fieldName, String fieldType) {
    Map<String, String> properties = new HashMap<>();
    properties.put("fieldName", fieldName);
    properties.put("fieldType", fieldType);
    return new ETLPlugin(NAME, BatchReduceAggregator.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("fieldName", new PluginPropertyField("fieldName", "", "string", true, true));
    properties.put("fieldType", new PluginPropertyField("fieldType", "", "string", true, true));
    return new PluginClass(BatchReduceAggregator.PLUGIN_TYPE, NAME, "",
                           FieldCountReduceAggregator.class.getName(), "config", properties);
  }
}
