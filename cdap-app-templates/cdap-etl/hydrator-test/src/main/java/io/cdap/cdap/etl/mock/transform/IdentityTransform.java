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
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Identity transform for testing.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("Identity")
public class IdentityTransform extends Transform<StructuredRecord, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    stageConfigurer.setOutputSchema(stageConfigurer.getInputSchema());
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    Schema schema = context.getInputSchema();
    if (schema != null && schema.getFields() != null) {
      schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList())
        .forEach(field -> context.record(Collections.singletonList(
          new FieldTransformOperation("Identity transform " + field, "Identity transform",
                                      Collections.singletonList(field), field))));
    }
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
    return PluginClass.builder().setName("Identity").setType(Transform.PLUGIN_TYPE)
             .setDescription("").setClassName(IdentityTransform.class.getName()).setProperties(properties)
             .build();
  }
}
