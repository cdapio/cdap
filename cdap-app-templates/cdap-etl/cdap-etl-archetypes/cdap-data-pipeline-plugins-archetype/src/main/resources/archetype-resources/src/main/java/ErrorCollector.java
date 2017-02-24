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

package $package;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.ErrorRecord;
import co.cask.cdap.etl.api.ErrorTransform;
import co.cask.cdap.etl.api.PipelineConfigurer;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Adds the error code and error message to each record, then emits it.
 */
@Plugin(type = ErrorTransform.PLUGIN_TYPE)
@Name("ErrorCollector")
public class ErrorCollector extends ErrorTransform<StructuredRecord, StructuredRecord> {
  private final Config config;

  public ErrorCollector(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (inputSchema != null) {
      if (inputSchema.getField(config.messageField) != null) {
        throw new IllegalArgumentException(String.format(
          "Input schema already contains message field %s. Please set messageField to a different value.",
          config.messageField));
      }
      if (inputSchema.getField(config.codeField) != null) {
        throw new IllegalArgumentException(String.format(
          "Input schema already contains code field %s. Please set codeField to a different value.",
          config.codeField));
      }
      if (inputSchema.getField(config.stageField) != null) {
        throw new IllegalArgumentException(String.format(
          "Input schema already contains stage field %s. Please set stageField to a different value.",
          config.stageField));
      }
      Schema outputSchema = getOutputSchema(config, inputSchema);
      pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
    }
  }

  @Override
  public void transform(ErrorRecord<StructuredRecord> input, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord invalidRecord = input.getRecord();
    StructuredRecord.Builder output = StructuredRecord.builder(getOutputSchema(config, invalidRecord.getSchema()));
    for (Schema.Field field : invalidRecord.getSchema().getFields()) {
      output.set(field.getName(), invalidRecord.get(field.getName()));
    }
    if (config.messageField != null) {
      output.set(config.messageField, input.getErrorMessage());
    }
    if (config.codeField != null) {
      output.set(config.codeField, input.getErrorCode());
    }
    if (config.stageField != null) {
      output.set(config.stageField, input.getStageName());
    }
    emitter.emit(output.build());
  }

  private static Schema getOutputSchema(Config config, Schema inputSchema) {
    List<Schema.Field> fields = new ArrayList<>();
    fields.addAll(inputSchema.getFields());
    if (config.messageField != null) {
      fields.add(Schema.Field.of(config.messageField, Schema.of(Schema.Type.STRING)));
    }
    if (config.codeField != null) {
      fields.add(Schema.Field.of(config.codeField, Schema.of(Schema.Type.INT)));
    }
    if (config.stageField != null) {
      fields.add(Schema.Field.of(config.stageField, Schema.of(Schema.Type.STRING)));
    }
    return Schema.recordOf("error" + inputSchema.getRecordName(), fields);
  }

  /**
   * The plugin config
   */
  public static class Config extends PluginConfig {
    @Nullable
    @Description("The name of the error message field to use in the output schema. " +
      "If this not specified, the error message will be dropped.")
    private String messageField;

    @Nullable
    @Description("The name of the error code field to use in the output schema. " +
      "If this not specified, the error code will be dropped.")
    private String codeField;

    @Nullable
    @Description("The name of the error stage field to use in the output schema. " +
      "If this not specified, the error stage will be dropped.")
    private String stageField;

  }
}
