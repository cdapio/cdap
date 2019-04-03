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
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.ErrorRecord;
import co.cask.cdap.etl.api.ErrorTransform;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.proto.v2.ETLPlugin;

import java.util.HashMap;
import java.util.Map;

/**
 * Filters out errors that have a specific error code
 */
@Plugin(type = ErrorTransform.PLUGIN_TYPE)
@Name("Filter")
public class FilterErrorTransform extends ErrorTransform<StructuredRecord, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private Config config;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    stageConfigurer.setOutputSchema(stageConfigurer.getInputSchema());
  }

  @Override
  public void transform(ErrorRecord<StructuredRecord> input, Emitter<StructuredRecord> emitter) throws Exception {
    if (input.getErrorCode() != config.code) {
      emitter.emit(input.getRecord());
    }
  }

  /**
   * Config for the error transform.
   */
  public static class Config extends PluginConfig {
    private int code;
  }

  public static ETLPlugin getPlugin(int code) {
    Map<String, String> properties = new HashMap<>();
    properties.put("code", String.valueOf(code));
    return new ETLPlugin("Filter", ErrorTransform.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("code", new PluginPropertyField("code", "", "int", true, false));
    return new PluginClass(ErrorTransform.PLUGIN_TYPE, "Filter", "", FilterErrorTransform.class.getName(),
                           "config", properties);
  }
}
