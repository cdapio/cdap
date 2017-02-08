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

package co.cask.cdap.etl.mock.transform;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.etl.proto.v2.ETLPlugin;

import java.util.HashMap;
import java.util.Map;

/**
 * Identity transform for testing.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("Identity")
public class IdentityTransform extends Transform<StructuredRecord, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    stageConfigurer.setOutputSchema(stageConfigurer.getInputSchema());
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    // should never happen, here to test app correctness in unit tests
    Schema inputSchema = context.getInputSchema();
    if (inputSchema != null && !inputSchema.equals(context.getOutputSchema())) {
      throw new IllegalStateException("runtime schema does not match what was set at configure time.");
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(input);
  }

  public static ETLPlugin getPlugin() {
    Map<String, String> properties = new HashMap<>();
    return new ETLPlugin("Identity", Transform.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    return new PluginClass(Transform.PLUGIN_TYPE, "Identity", "", IdentityTransform.class.getName(),
                           null, properties);
  }
}
