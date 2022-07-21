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

package io.cdap.cdap.etl.mock.transform;

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
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transform which adds prefix to all the fields of structured record for testing multi input schemas
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("FieldsPrefixTransform")
public class FieldsPrefixTransform extends Transform<StructuredRecord, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();

  private final Config config;

  public FieldsPrefixTransform(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    try {
      Schema outSchema = config.getOutputSchema(Schema.parseJson(config.schemaStr));
      stageConfigurer.setOutputSchema(outSchema);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid output schema: " + e.getMessage(), e);
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    Schema outSchema = config.getOutputSchema(input.getSchema());
    StructuredRecord.Builder outputBuilder = StructuredRecord.builder(outSchema);

    for (Schema.Field inField : input.getSchema().getFields()) {
        outputBuilder.set(config.prefix + inField.getName(), input.get(inField.getName()));
    }
    emitter.emit(outputBuilder.build());
  }

  /**
   * Config for join plugin
   */
  public static class Config extends PluginConfig {
    private String prefix;
    private String schemaStr;

    public Config() {
      prefix = "prefix";
      schemaStr = "schemaStr";
    }

    private Schema getOutputSchema(Schema inputSchema) {
      List<Schema.Field> outFields = new ArrayList<>();
      List<Schema.Field> fields = inputSchema.getFields();
      for (Schema.Field field : fields) {
        outFields.add(Schema.Field.of(prefix + field.getName(), field.getSchema()));
      }
      return Schema.recordOf("prefixed.outfields", outFields);
    }
  }

  public static ETLPlugin getPlugin(String prefix, String schemaStr) {
    Map<String, String> properties = new HashMap<>();
    properties.put("prefix", prefix);
    properties.put("schemaStr", schemaStr);
    return new ETLPlugin("FieldsPrefixTransform", Transform.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("prefix", new PluginPropertyField("prefix", "", "string", true, false));
    properties.put("schemaStr", new PluginPropertyField("schemaStr", "", "string", true, false));
    return PluginClass.builder().setName("FieldsPrefixTransform").setType(Transform.PLUGIN_TYPE)
             .setDescription("").setClassName(FieldsPrefixTransform.class.getName()).setProperties(properties)
             .setConfigFieldName("config").build();
  }

}
