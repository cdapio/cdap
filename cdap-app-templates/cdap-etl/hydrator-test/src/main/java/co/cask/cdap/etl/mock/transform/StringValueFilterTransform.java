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

package co.cask.cdap.etl.mock.transform;

import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.proto.v2.ETLPlugin;

import java.util.HashMap;
import java.util.Map;

/**
 * Transform that filters out records whose configured field is a configured value.
 * For example, can filter all records whose 'foo' field is equal to 'bar'. Assumes the field is of type string.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("StringValueFilter")
public class StringValueFilterTransform extends Transform<StructuredRecord, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Config config;

  public StringValueFilterTransform(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    stageConfigurer.setOutputSchema(stageConfigurer.getInputSchema());
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    String value = input.get(config.field);
    if (!config.value.equals(value)) {
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
