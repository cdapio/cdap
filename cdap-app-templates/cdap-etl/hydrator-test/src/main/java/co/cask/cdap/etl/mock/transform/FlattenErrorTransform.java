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
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.ErrorRecord;
import co.cask.cdap.etl.api.ErrorTransform;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.etl.proto.v2.ETLPlugin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Adds the error code and error message to each record, then emits it.
 */
@Plugin(type = ErrorTransform.PLUGIN_TYPE)
@Name("Flatten")
public class FlattenErrorTransform extends ErrorTransform<StructuredRecord, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    if (inputSchema != null) {
      stageConfigurer.setOutputSchema(getOutputSchema(inputSchema));
    }
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    Schema inputSchema = context.getInputSchema();
    Schema outputSchema = context.getOutputSchema();
    if (inputSchema != null && !getOutputSchema(inputSchema).equals(outputSchema)) {
      // should never happen, here for unit tests
      throw new IllegalStateException("Output schema is not correct.");
    }
  }

  @Override
  public void transform(ErrorRecord<StructuredRecord> input, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord invalidRecord = input.getRecord();
    StructuredRecord.Builder output = StructuredRecord.builder(getOutputSchema(invalidRecord.getSchema()));
    for (Schema.Field field : invalidRecord.getSchema().getFields()) {
      output.set(field.getName(), invalidRecord.get(field.getName()));
    }
    emitter.emit(output.set("errMsg", input.getErrorMessage())
                   .set("errCode", input.getErrorCode())
                   .set("errStage", input.getStageName())
                   .build());
  }

  private Schema getOutputSchema(Schema inputSchema) {
    List<Schema.Field> fields = new ArrayList<>();
    fields.addAll(inputSchema.getFields());
    fields.add(Schema.Field.of("errMsg", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    fields.add(Schema.Field.of("errCode", Schema.nullableOf(Schema.of(Schema.Type.INT))));
    fields.add(Schema.Field.of("errStage", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    return Schema.recordOf("error" + inputSchema.getRecordName(), fields);
  }

  public static ETLPlugin getPlugin() {
    return new ETLPlugin("Flatten", ErrorTransform.PLUGIN_TYPE, new HashMap<String, String>(), null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    return new PluginClass(ErrorTransform.PLUGIN_TYPE, "Flatten", "", FlattenErrorTransform.class.getName(),
                           null, properties);
  }
}
