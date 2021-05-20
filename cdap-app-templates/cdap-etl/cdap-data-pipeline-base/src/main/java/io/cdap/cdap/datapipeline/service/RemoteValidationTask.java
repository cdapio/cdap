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

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskSystemAppContext;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.batch.BatchPipelineSpec;
import io.cdap.cdap.etl.batch.BatchPipelineSpecGenerator;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.ConnectionMacroEvaluator;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.DefaultPipelineConfigurer;
import io.cdap.cdap.etl.common.DefaultStageConfigurer;
import io.cdap.cdap.etl.common.OAuthMacroEvaluator;
import io.cdap.cdap.etl.common.SecureStoreMacroEvaluator;
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
import io.cdap.cdap.proto.id.NamespaceId;

import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

/**
 * RemoteValidationTask
 */
public class RemoteValidationTask implements RunnableTask {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .serializeNulls()
    .create();

  @Override
  public void run(RunnableTaskContext context) throws Exception {
    RunnableTaskSystemAppContext systemAppContext = context.getSystemAppContext();
    RemoteValidationRequest remoteValidationRequest = GSON.fromJson(context.getParam(), RemoteValidationRequest.class);
    String namespace = remoteValidationRequest.getNamespace();
    if (!systemAppContext.getAdmin().namespaceExists(namespace)) {
      throw new IllegalArgumentException(String.format("Namespace '%s' does not exist", namespace));
    }

    String validationRequestString = remoteValidationRequest.getValidationRequest();
    StageValidationRequest validationRequest;
    try {
      validationRequest = GSON.fromJson(validationRequestString,
                                        StageValidationRequest.class);
      validationRequest.validate();
    } catch (JsonSyntaxException e) {
      throw new IllegalArgumentException(String.format("Unable to decode request body %s", validationRequestString), e);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid stage config", e);
    }

    ETLStage stageConfig = validationRequest.getStage();
    ValidatingConfigurer validatingConfigurer =
      new ValidatingConfigurer(systemAppContext.createPluginConfigurer(NamespaceId.SYSTEM.getNamespace()));
    // Batch or Streaming doesn't matter for a single stage.
    PipelineSpecGenerator<ETLBatchConfig, BatchPipelineSpec> pipelineSpecGenerator =
      new BatchPipelineSpecGenerator(validatingConfigurer, Collections.emptySet(), Collections.emptySet(),
                                     Engine.SPARK);

    DefaultStageConfigurer stageConfigurer = new DefaultStageConfigurer(stageConfig.getName());
    for (StageSchema stageSchema : validationRequest.getInputSchemas()) {
      stageConfigurer.addInputSchema(stageSchema.getStage(), stageSchema.getSchema());
      stageConfigurer.addInputStage(stageSchema.getStage());
    }
    DefaultPipelineConfigurer pipelineConfigurer =
      new DefaultPipelineConfigurer(validatingConfigurer, stageConfig.getName(), Engine.SPARK, stageConfigurer);

    Map<String, String> arguments = Collections.emptyMap();

    // Fetch preferences for this instance and namespace and use them as program arguments if the user selects
    // this option.
    if (validationRequest.getResolveMacrosFromPreferences()) {
      try {
        arguments = systemAppContext.getPreferencesForNamespace(namespace, true);
      } catch (IllegalArgumentException iae) {
        // If this method returns IllegalArgumentException, it means the namespace doesn't exist.
        // If this is the case, we return a 404 error.
        throw new IllegalArgumentException(String.format("Namespace '%s' does not exist", namespace), iae);
      }
    }

    // evaluate secure macros
    Map<String, MacroEvaluator> evaluators = ImmutableMap.of(
      SecureStoreMacroEvaluator.FUNCTION_NAME,
      new SecureStoreMacroEvaluator(namespace, systemAppContext.getSecureStore()),
      OAuthMacroEvaluator.FUNCTION_NAME, new OAuthMacroEvaluator(systemAppContext.getServiceDiscoverer()),
      ConnectionMacroEvaluator.FUNCTION_NAME,
      new ConnectionMacroEvaluator(namespace, systemAppContext.getServiceDiscoverer())
    );
    MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(new BasicArguments(arguments), evaluators);

    Map<String, String> evaluatedProperties = systemAppContext
      .evaluateMacros(namespace, stageConfig.getPlugin().getProperties(), macroEvaluator,
                      MacroParserOptions.builder()
                        .skipInvalidMacros()
                        .setEscaping(false)
                        .setFunctionWhitelist(evaluators.keySet())
                        .build());
    ETLPlugin originalConfig = stageConfig.getPlugin();
    ETLPlugin evaluatedConfig = new ETLPlugin(originalConfig.getName(), originalConfig.getType(),
                                              evaluatedProperties, originalConfig.getArtifactConfig());

    try {
      StageSpec spec = pipelineSpecGenerator.configureStage(stageConfig.getName(), evaluatedConfig,
                                                            pipelineConfigurer).build();
      context.writeResult(GSON.toJson(new StageValidationResponse(spec)).getBytes());
    } catch (ValidationException e) {
      context.writeResult(GSON.toJson(new StageValidationResponse(e.getFailures())).getBytes());
    }
  }
}
