/*
 * Copyright © 2019 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.datapipeline.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.service.http.AbstractSystemHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.batch.BatchPipelineSpec;
import io.cdap.cdap.etl.batch.BatchPipelineSpecGenerator;
import io.cdap.cdap.etl.common.DefaultPipelineConfigurer;
import io.cdap.cdap.etl.common.DefaultStageConfigurer;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.proto.v2.spec.PipelineSpec;
import io.cdap.cdap.etl.proto.v2.spec.PluginSpec;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.proto.v2.validation.StageSchema;
import io.cdap.cdap.etl.proto.v2.validation.StageValidationRequest;
import io.cdap.cdap.etl.proto.v2.validation.StageValidationResponse;
import io.cdap.cdap.etl.spec.PipelineSpecGenerator;
import io.cdap.cdap.etl.validation.ValidatingConfigurer;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.artifact.AppRequest;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handles validation logic for pipelines.
 */
public class ValidationHandler extends AbstractSystemHttpServiceHandler {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private static final Type APP_REQUEST_TYPE = new TypeToken<AppRequest<JsonObject>>() { }.getType();
  private static final String ARTIFACT_BATCH_NAME = "cdap-data-pipeline";
  private static final String ARTIFACT_STREAMING_NAME = "cdap-data-streams";

  @GET
  @Path("v1/health")
  public void healthCheck(HttpServiceRequest request, HttpServiceResponder responder) {
    responder.sendStatus(HttpURLConnection.HTTP_OK);
  }

  @POST
  @Path("v1/contexts/{context}/validations/stage")
  public void validateStage(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespace) throws IOException {
    if (!getContext().getAdmin().namespaceExists(namespace)) {
      responder.sendError(HttpURLConnection.HTTP_NOT_FOUND, String.format("Namespace '%s' does not exist", namespace));
      return;
    }

    StageValidationRequest validationRequest;
    try {
      validationRequest = GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(),
                                        StageValidationRequest.class);
      validationRequest.validate();
    } catch (JsonSyntaxException e) {
      responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Unable to decode request body: " + e.getMessage());
      return;
    } catch (IllegalArgumentException e) {
      responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Invalid stage config: " + e.getMessage());
      return;
    }

    ETLStage stageConfig = validationRequest.getStage();
    ValidatingConfigurer validatingConfigurer =
      new ValidatingConfigurer(getContext().createPluginConfigurer(namespace));
    // Batch or Streaming doesn't matter for a single stage.
    PipelineSpecGenerator<ETLBatchConfig, BatchPipelineSpec> pipelineSpecGenerator =
      new BatchPipelineSpecGenerator(validatingConfigurer, Collections.emptySet(), Collections.emptySet(),
                                     Engine.SPARK);

    DefaultStageConfigurer stageConfigurer = new DefaultStageConfigurer(stageConfig.getName());
    for (StageSchema stageSchema : validationRequest.getInputSchemas()) {
      stageConfigurer.addInputSchema(stageSchema.getStage(), stageSchema.getSchema());
    }
    DefaultPipelineConfigurer pipelineConfigurer =
      new DefaultPipelineConfigurer(validatingConfigurer, stageConfig.getName(), Engine.SPARK, stageConfigurer);

    // evaluate secure macros
    MacroEvaluator macroEvaluator = new SecureStoreMacroEvaluator(namespace, getContext());
    Map<String, String> evaluatedProperties = getContext()
      .evaluateMacros(namespace, stageConfig.getPlugin().getProperties(), macroEvaluator,
                      MacroParserOptions.builder()
                        .skipInvalidMacros()
                        .setEscaping(false)
                        .setFunctionWhitelist("secure")
                        .build());
    ETLPlugin originalConfig = stageConfig.getPlugin();
    ETLPlugin evaluatedConfig = new ETLPlugin(originalConfig.getName(), originalConfig.getType(),
                                              evaluatedProperties, originalConfig.getArtifactConfig());

    try {
      StageSpec spec = pipelineSpecGenerator.configureStage(stageConfig.getName(), evaluatedConfig,
                                                            pipelineConfigurer).build();
      responder.sendString(GSON.toJson(new StageValidationResponse(spec)));
    } catch (ValidationException e) {
      responder.sendString(GSON.toJson(new StageValidationResponse(e.getFailures())));
    }
  }

  @POST
  @Path("v1/contexts/{context}/validations/pipeline")
  public void validatePipeline(HttpServiceRequest request, HttpServiceResponder responder,
                               @PathParam("context") String namespace) throws IOException {
    if (!getContext().getAdmin().namespaceExists(namespace)) {
      responder.sendError(HttpURLConnection.HTTP_NOT_FOUND, String.format("Namespace '%s' does not exist", namespace));
      return;
    }

    AppRequest<JsonObject> appRequest;
    try {
      appRequest = GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(), APP_REQUEST_TYPE);
      appRequest.validate();
    } catch (JsonSyntaxException e) {
      responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Unable to decode request body: " + e.getMessage());
      return;
    } catch (IllegalArgumentException e) {
      responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST,
                          "Invalid artifact in pipeline request: " + e.getMessage());
      return;
    }

    ArtifactSummary artifactSummary = appRequest.getArtifact();
    String artifactName = artifactSummary.getName();
    if (ARTIFACT_BATCH_NAME.equals(artifactName)) {
      responder.sendJson(validateBatchPipeline(appRequest));
    } else if (ARTIFACT_STREAMING_NAME.equals(artifactName)) {
      responder.sendJson(validateStreamingPipeline(appRequest));
    } else {
      responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST,
                          String.format("Invalid artifact '%s'. Must be '%s' or '%s'.", artifactName,
                                        ARTIFACT_BATCH_NAME, ARTIFACT_STREAMING_NAME));
    }
  }

  // TODO: (CDAP-14687) implement with real logic
  private BatchPipelineSpec validateBatchPipeline(AppRequest<JsonObject> appRequest) {
    return BatchPipelineSpec.builder().addStage(getDummyStageSpec()).build();
  }

  private PipelineSpec validateStreamingPipeline(AppRequest<JsonObject> appRequest) {
    return PipelineSpec.builder().addStage(getDummyStageSpec()).build();
  }

  // TODO: (CDAP-14686, CDAP-14687) remove once real logic is implemented
  private StageSpec getDummyStageSpec() {
    PluginSpec pluginSpec = new PluginSpec(BatchSource.PLUGIN_TYPE, "file", Collections.emptyMap(),
                                           new ArtifactId("core-plugins", new ArtifactVersion("2.2.0"),
                                                          ArtifactScope.SYSTEM));
    return StageSpec.builder("dummy", pluginSpec).build();
  }
}
