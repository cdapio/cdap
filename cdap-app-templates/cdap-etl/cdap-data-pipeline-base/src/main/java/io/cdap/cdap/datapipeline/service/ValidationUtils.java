/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.datapipeline.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.batch.BatchPipelineSpec;
import io.cdap.cdap.etl.batch.BatchPipelineSpecGenerator;
import io.cdap.cdap.etl.common.DefaultPipelineConfigurer;
import io.cdap.cdap.etl.common.DefaultStageConfigurer;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.proto.v2.validation.StageSchema;
import io.cdap.cdap.etl.proto.v2.validation.StageValidationRequest;
import io.cdap.cdap.etl.proto.v2.validation.StageValidationResponse;
import io.cdap.cdap.etl.spec.PipelineSpecGenerator;
import io.cdap.cdap.etl.validation.ValidatingConfigurer;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

/**
 * Utility functions for common pipeline validation code
 */
public final class ValidationUtils {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .serializeNulls()
    .create();

  private ValidationUtils() {
  }

  /**
   * Validate plugin based on the {@link StageValidationRequest}
   *
   * @param validationRequest {@link StageValidationRequest} with plugin properties
   * @param pluginConfigurer  {@link PluginConfigurer} for using the plugin
   * @param macroFn           {@link Function} for evaluating macros
   * @return {@link StageValidationResponse} in json format
   */
  public static StageValidationResponse validate(String namespace, StageValidationRequest validationRequest,
                                                 PluginConfigurer pluginConfigurer,
                                                 Function<Map<String, String>, Map<String, String>> macroFn) {

    ETLStage stageConfig = validationRequest.getStage();
    ValidatingConfigurer validatingConfigurer = new ValidatingConfigurer(pluginConfigurer);
    // Batch or Streaming doesn't matter for a single stage.
    PipelineSpecGenerator<ETLBatchConfig, BatchPipelineSpec> pipelineSpecGenerator =
      new BatchPipelineSpecGenerator(namespace, validatingConfigurer, null, Collections.emptySet(),
                                     Collections.emptySet(), Engine.SPARK);

    DefaultStageConfigurer stageConfigurer = new DefaultStageConfigurer(stageConfig.getName());
    for (StageSchema stageSchema : validationRequest.getInputSchemas()) {
      stageConfigurer.addInputSchema(stageSchema.getStage(), stageSchema.getSchema());
      stageConfigurer.addInputStage(stageSchema.getStage());
    }
    DefaultPipelineConfigurer pipelineConfigurer =
      new DefaultPipelineConfigurer(validatingConfigurer, stageConfig.getName(), Engine.SPARK, stageConfigurer);

    // evaluate macros
    Map<String, String> evaluatedProperties = macroFn.apply(stageConfig.getPlugin().getProperties());

    ETLPlugin originalConfig = stageConfig.getPlugin();
    ETLPlugin evaluatedConfig = new ETLPlugin(originalConfig.getName(), originalConfig.getType(),
                                              evaluatedProperties, originalConfig.getArtifactConfig());
    try {
      StageSpec spec = pipelineSpecGenerator.configureStage(stageConfig.getName(), evaluatedConfig,
                                                            pipelineConfigurer).build();
      return new StageValidationResponse(spec);
    } catch (ValidationException e) {
      return new StageValidationResponse(e.getFailures());
    }
  }
}
