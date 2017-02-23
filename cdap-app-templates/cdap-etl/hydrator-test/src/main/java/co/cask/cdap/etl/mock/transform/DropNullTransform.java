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

package co.cask.cdap.etl.mock.transform;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.proto.v2.ETLPlugin;

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
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
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
    return new PluginClass(Transform.PLUGIN_TYPE, "DropField", "", DropNullTransform.class.getName(),
                           "config", properties);
  }
}
