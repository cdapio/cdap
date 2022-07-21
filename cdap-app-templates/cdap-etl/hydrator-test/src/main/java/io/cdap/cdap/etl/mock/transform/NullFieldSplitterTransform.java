/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.etl.mock.transform;

import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.MultiOutputEmitter;
import io.cdap.cdap.etl.api.MultiOutputPipelineConfigurer;
import io.cdap.cdap.etl.api.MultiOutputStageConfigurer;
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;

import java.util.HashMap;
import java.util.Map;

/**
 * Splitter transform that sends all records that have a null value for a configurable field to one output port and all
 * other records to another port.
 */
@Plugin(type = SplitterTransform.PLUGIN_TYPE)
@Name("NullFieldSplitter")
public class NullFieldSplitterTransform extends SplitterTransform<StructuredRecord, StructuredRecord> {

  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Config config;

  public NullFieldSplitterTransform(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(MultiOutputPipelineConfigurer multiOutputPipelineConfigurer) {
    MultiOutputStageConfigurer stageConfigurer = multiOutputPipelineConfigurer.getMultiOutputStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    if (inputSchema != null && inputSchema.getField(config.field) == null) {
      throw new IllegalArgumentException("Field " + config.field + " is not in the input schema.");
    }

    Map<String, Schema> portSchemas = new HashMap<>();
    portSchemas.put("non-null", inputSchema);
    portSchemas.put("null", inputSchema);
    stageConfigurer.setOutputSchemas(portSchemas);
  }

  @Override
  public void transform(StructuredRecord input, MultiOutputEmitter<StructuredRecord> emitter) throws Exception {
    String port = input.get(config.field) == null ? "null" : "non-null";
    emitter.emit(port, input);
  }

  /**
   * Config for the transform.
   */
  public static class Config extends PluginConfig {

    @Macro
    private String field;
  }

  public static ETLPlugin getPlugin(String field) {
    Map<String, String> properties = new HashMap<>();
    properties.put("field", field);
    return new ETLPlugin("NullFieldSplitter", SplitterTransform.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("field", new PluginPropertyField("field", "", "string", true, true));
    return PluginClass.builder().setName("NullFieldSplitter").setType(SplitterTransform.PLUGIN_TYPE)
             .setDescription("").setClassName(NullFieldSplitterTransform.class.getName()).setProperties(properties)
             .setConfigFieldName("config").build();
  }
}
