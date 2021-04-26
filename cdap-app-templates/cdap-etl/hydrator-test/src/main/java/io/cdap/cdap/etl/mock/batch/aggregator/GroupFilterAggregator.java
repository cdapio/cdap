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

package io.cdap.cdap.etl.mock.batch.aggregator;

import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Aggregator that emits errors for all records that have a specific group key.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("GroupFilter")
public class GroupFilterAggregator extends BatchAggregator<String, StructuredRecord, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private Config config;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    if (inputSchema == null) {
      return;
    }
    Schema.Field groupField = inputSchema.getField(config.field);
    if (groupField == null) {
      throw new IllegalArgumentException(config.field + " is not in the input schema");
    }
    Schema groupSchema = groupField.getSchema();
    Schema.Type groupType = groupSchema.isNullable() ? groupSchema.getNonNullable().getType() : groupSchema.getType();
    if (groupType != Schema.Type.STRING) {
      throw new IllegalArgumentException(config.field + " is not of type string");
    }
    stageConfigurer.setOutputSchema(inputSchema);
  }

  @Override
  public void groupBy(StructuredRecord input, Emitter<String> emitter) throws Exception {
    String val = input.get(config.field);
    if (val != null) {
      emitter.emit(val);
    }
  }

  @Override
  public void aggregate(String groupKey, Iterator<StructuredRecord> groupValues,
                        Emitter<StructuredRecord> emitter) throws Exception {
    if (config.value.equals(groupKey)) {
      while (groupValues.hasNext()) {
        emitter.emitError(new InvalidEntry<>(3, "bad val", groupValues.next()));
      }
    } else {
      while (groupValues.hasNext()) {
        emitter.emit(groupValues.next());
      }
    }
  }

  /**
   * The plugin config
   */
  public static class Config extends PluginConfig {
    String field;
    String value;
  }

  public static ETLPlugin getPlugin(String field, String val) {
    Map<String, String> properties = new HashMap<>();
    properties.put("field", field);
    properties.put("value", val);
    return new ETLPlugin("GroupFilter", BatchAggregator.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("field", new PluginPropertyField("field", "", "string", true, false));
    properties.put("value", new PluginPropertyField("value", "", "string", true, false));
    return PluginClass.builder().setName("GroupFilter").setType(BatchAggregator.PLUGIN_TYPE)
             .setDescription("").setClassName(GroupFilterAggregator.class.getName()).setProperties(properties)
             .setConfigFieldName("config").build();
  }
}
