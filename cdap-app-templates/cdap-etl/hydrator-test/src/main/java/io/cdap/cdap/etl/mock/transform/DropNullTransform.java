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
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transform that drops a configurable field from all input records if it has a null value.
 * The the value is not null, it is emitted as an error
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("DropField")
public class DropNullTransform extends Transform<StructuredRecord, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Config config;

  public DropNullTransform(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    if (inputSchema != null) {
      stageConfigurer.setOutputSchema(getOutputSchema(inputSchema));
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord.Builder builder = StructuredRecord.builder(getOutputSchema(input.getSchema()));
    for (Schema.Field field : input.getSchema().getFields()) {
      String fieldName = field.getName();
      Object val = input.get(fieldName);
      if (fieldName.equals(config.field) && val != null) {
        emitter.emitError(new InvalidEntry<>(5, "Field " + config.field + " was not null", input));
        return;
      }
      builder.set(fieldName, input.get(fieldName));
    }
    emitter.emit(builder.build());
  }

  private Schema getOutputSchema(Schema inputSchema) {
    List<Schema.Field> fields = new ArrayList<>();
    for (Schema.Field inputField : inputSchema.getFields()) {
      if (inputField.getName().equals(config.field)) {
        continue;
      }
      fields.add(inputField);
    }
    return Schema.recordOf(inputSchema.getRecordName() + ".short", fields);
  }

  /**
   * Config for the transform.
   */
  public static class Config extends PluginConfig {
    private String field;
  }

  public static ETLPlugin getPlugin(String field) {
    Map<String, String> properties = new HashMap<>();
    properties.put("field", field);
    return new ETLPlugin("DropField", Transform.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("field", new PluginPropertyField("field", "", "string", true, false));
    return PluginClass.builder().setName("DropField").setType(Transform.PLUGIN_TYPE)
             .setDescription("").setClassName(DropNullTransform.class.getName()).setProperties(properties)
             .setConfigFieldName("config").build();
  }
}
