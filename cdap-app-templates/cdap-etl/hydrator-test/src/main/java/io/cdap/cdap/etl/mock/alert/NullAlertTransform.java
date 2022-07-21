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

package io.cdap.cdap.etl.mock.alert;

import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;

import java.util.HashMap;
import java.util.Map;

/**
 * Checks if a particular field is null, emitting that record as an alert if so. Otherwise passes records on
 * exactly as they came in.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(NullAlertTransform.NAME)
public class NullAlertTransform extends Transform<StructuredRecord, StructuredRecord> {
  public static final String NAME = "NullAlert";
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Conf conf;

  public NullAlertTransform(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    stageConfigurer.setOutputSchema(stageConfigurer.getInputSchema());
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    if (input.get(conf.field) == null) {
      emitter.emitAlert(new HashMap<String, String>());
    } else {
      emitter.emit(input);
    }
  }

  /**
   * Config for plugin
   */
  public static class Conf extends PluginConfig {
    private String field;
  }

  public static ETLPlugin getPlugin(String field) {
    Map<String, String> properties = new HashMap<>();
    properties.put("field", field);
    return new ETLPlugin(NAME, Transform.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("field", new PluginPropertyField("field", "", "string", true, false));
    return PluginClass.builder().setName(NAME).setType(Transform.PLUGIN_TYPE)
             .setDescription("").setClassName(NullAlertTransform.class.getName()).setProperties(properties)
             .setConfigFieldName("conf").build();
  }
}
