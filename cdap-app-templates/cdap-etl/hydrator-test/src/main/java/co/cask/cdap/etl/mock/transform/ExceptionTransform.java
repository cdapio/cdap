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
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

/**
 * An transform that throws exception if expected condition is not met.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(ExceptionTransform.NAME)
public class ExceptionTransform extends Transform<StructuredRecord, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  public static final String NAME = "Exception";
  private static final String EXPECTED_FIELD = "expectedField";
  private static final String EXPECTED_VALUE = "expectedValue";

  private Map<String, String> properties;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    stageConfigurer.setOutputSchema(stageConfigurer.getInputSchema());
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    properties = context.getPluginProperties().getProperties();
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    String expectedField = properties.get(EXPECTED_FIELD);
    String expectedValue = properties.get(EXPECTED_VALUE);
    if (expectedField == null || !input.get(expectedField).equals(expectedValue)) {
      throw new IllegalArgumentException("Not getting expected value");
    }
    emitter.emit(input);
  }

  public static ETLPlugin getPlugin(String expectedField, String expectedValue) {
    Map<String, String> properties = new HashMap<>();
    properties.put(EXPECTED_FIELD, expectedField);
    properties.put(EXPECTED_VALUE, expectedValue);
    return new ETLPlugin(NAME, Transform.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    return new PluginClass(Transform.PLUGIN_TYPE, NAME, "", IdentityTransform.class.getName(), null,
                           ImmutableMap.<String, PluginPropertyField>of());
  }
}
