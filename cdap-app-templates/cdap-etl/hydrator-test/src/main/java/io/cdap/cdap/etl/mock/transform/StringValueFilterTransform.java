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

package io.cdap.cdap.etl.mock.transform;

import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;

import java.util.HashMap;
import java.util.Map;

/**
 * Transform that filters out records whose configured field is a configured value.
 * For example, can filter all records whose 'foo' field is equal to 'bar'. Assumes the field is of type string.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(StringValueFilterTransform.NAME)
public class StringValueFilterTransform extends Transform<StructuredRecord, StructuredRecord> {
  public static final String NAME = "StringValueFilter";
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Config config;
  private String filterField;
  private String filterValue;

  public StringValueFilterTransform(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    if (inputSchema != null && !config.containsMacro("field")) {
      Schema.Field field = inputSchema.getField(config.field);
      if (field == null) {
        collector.addFailure(String.format("'%s' is not a field in the input schema.", config.field),
                             "Make sure field is present in the input schema.")
          .withConfigProperty("field");
        collector.getOrThrowException();
      }
      Schema fieldSchema = field.getSchema();
      Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
      if (fieldType != Schema.Type.STRING) {
        collector.addFailure(String.format("'%s' is of type '%s' instead of a string.", config.field, fieldType),
                             "Make sure provided field is of type string.")
          .withConfigProperty("field").withInputSchemaField(config.field);
      }
    }
    stageConfigurer.setOutputSchema(stageConfigurer.getInputSchema());
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    // override whatever is in the conf if they are specified in the pipeline arguments.
    super.initialize(context);
    filterField = context.getArguments().get("field");
    if (filterField == null)  {
      filterField = config.field;
    }
    filterValue = context.getArguments().get("value");
    if (filterValue == null) {
      filterValue = config.value;
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    String value = input.get(filterField);
    if (!filterValue.equals(value)) {
      emitter.emit(input);
    } else {
      emitter.emitError(new InvalidEntry<>(1, "bad string value", input));
    }
  }

  /**
   * Config for the transform.
   */
  public static class Config extends PluginConfig {
    @Macro
    private String field;

    @Macro
    private String value;
  }

  public static ETLPlugin getPlugin(String field, String value) {
    Map<String, String> properties = new HashMap<>();
    properties.put("field", field);
    properties.put("value", value);
    return new ETLPlugin("StringValueFilter", Transform.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("field", new PluginPropertyField("field", "", "string", true, true));
    properties.put("value", new PluginPropertyField("value", "", "string", true, true));
    return new PluginClass(Transform.PLUGIN_TYPE, "StringValueFilter", "", StringValueFilterTransform.class.getName(),
                           "config", properties);
  }
}
