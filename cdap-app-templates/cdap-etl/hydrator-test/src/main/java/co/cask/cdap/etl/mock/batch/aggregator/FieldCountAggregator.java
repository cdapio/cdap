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

package co.cask.cdap.etl.mock.batch.aggregator;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchAggregatorContext;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.proto.v2.ETLPlugin;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Groups on a specific field and adds count field. Used to test that the right values are going to the
 * right groups, to test multiple group keys for the same value, and to test setting the group key class
 * at runtime, and to test setting a supported non-writable class.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("FieldCount")
public class FieldCountAggregator extends BatchAggregator<Object, StructuredRecord, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Config config;
  private Schema schema;

  public FieldCountAggregator(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    stageConfigurer.setOutputSchema(config.getSchema());
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
  public void aggregate(Object groupKey, Iterator<StructuredRecord> groupValues,
                        Emitter<StructuredRecord> emitter) throws Exception {
    long count = 0;
    while (groupValues.hasNext()) {
      groupValues.next();
      count++;
    }
    emitter.emit(StructuredRecord.builder(schema)
                   .set(config.fieldName, groupKey)
                   .set("ct", count)
                   .build());
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    schema = config.getSchema();
  }

  /**
   * Conf for the aggregator.
   */
  public static class Config extends PluginConfig {
    private final String fieldName;

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
    return new ETLPlugin("FieldCount", BatchAggregator.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("fieldName", new PluginPropertyField("fieldName", "", "string", true));
    properties.put("fieldType", new PluginPropertyField("fieldType", "", "string", true));
    return new PluginClass(BatchAggregator.PLUGIN_TYPE, "FieldCount", "", FieldCountAggregator.class.getName(),
                           "config", properties);
  }
}
