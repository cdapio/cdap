/*
 * Copyright © 2019-2021 Cask Data, Inc.
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

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.api.service.http.AbstractSystemHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.worker.RemoteExecutionException;
import io.cdap.cdap.api.service.worker.RemoteTaskException;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.batch.BatchPipelineSpec;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.ConnectionMacroEvaluator;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.OAuthMacroEvaluator;
import io.cdap.cdap.etl.common.SecureStoreMacroEvaluator;
import io.cdap.cdap.etl.proto.v2.spec.PipelineSpec;
import io.cdap.cdap.etl.proto.v2.spec.PluginSpec;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.proto.v2.validation.StageValidationRequest;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.artifact.AppRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
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
    .serializeNulls()
    .create();
  private static final Type APP_REQUEST_TYPE = new TypeToken<AppRequest<JsonObject>>() {
  }.getType();
  private static final Logger LOG = LoggerFactory.getLogger(ValidationHandler.class);

  @GET
  @Path("v1/health")
  public void healthCheck(HttpServiceRequest request, HttpServiceResponder responder) {
    responder.sendStatus(HttpURLConnection.HTTP_OK);
  }

  @POST
  @Path("v1/contexts/{context}/validations/stage")
  public void validateStage(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespace) throws IOException, AccessException {
    if (!getContext().getAdmin().namespaceExists(namespace)) {
      responder.sendError(HttpURLConnection.HTTP_NOT_FOUND, String.format("Namespace '%s' does not exist", namespace));
      return;
    }

    //Validate remotely if remote execution is enabled
    if (getContext().isRemoteTaskEnabled()) {
      validateRemotely(request, responder, namespace);
      return;
    }
    validateLocally(request, responder, namespace);
  }

  private void validateRemotely(HttpServiceRequest request, HttpServiceResponder responder,
                                String namespace) throws IOException {
    String validationRequestString = StandardCharsets.UTF_8.decode(request.getContent()).toString();
    RemoteValidationRequest remoteValidationRequest = new RemoteValidationRequest(namespace, validationRequestString);
    RunnableTaskRequest runnableTaskRequest = RunnableTaskRequest.getBuilder(RemoteValidationTask.class.getName()).
      withParam(GSON.toJson(remoteValidationRequest)).
      build();
    try {
      byte[] bytes = getContext().runTask(runnableTaskRequest);
      responder.sendString(Bytes.toString(bytes));
    } catch (RemoteExecutionException e) {
      RemoteTaskException remoteTaskException = e.getCause();
      responder.sendError(
        getExceptionCode(remoteTaskException.getRemoteExceptionClassName(), remoteTaskException.getMessage(),
                         namespace), remoteTaskException.getMessage());
    } catch (Exception e) {
      responder.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
    }
  }

  private int getExceptionCode(String exceptionClass, String exceptionMessage, String namespace) {
    if (IllegalArgumentException.class.getName().equals(exceptionClass)) {
      return String.format(RemoteValidationTask.NAMESPACE_DOES_NOT_EXIST, namespace).equals(exceptionMessage) ?
        HttpURLConnection.HTTP_NOT_FOUND : HttpURLConnection.HTTP_BAD_REQUEST;
    }
    return HttpURLConnection.HTTP_INTERNAL_ERROR;
  }

  private void validateLocally(HttpServiceRequest request, HttpServiceResponder responder,
                               String namespace) throws IOException {
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

    Map<String, String> arguments = Collections.emptyMap();
    // Fetch preferences for this instance and namespace and use them as program arguments if the user selects
    // this option.
    if (validationRequest.getResolveMacrosFromPreferences()) {
      try {
        arguments = getContext().getPreferencesForNamespace(namespace, true);
      } catch (IllegalArgumentException iae) {
        // If this method returns IllegalArgumentException, it means the namespace doesn't exist.
        // If this is the case, we return a 404 error.
        responder.sendError(HttpURLConnection.HTTP_NOT_FOUND,
                            String.format("Namespace '%s' does not exist", namespace));
        return;
      }
    }

    Map<String, MacroEvaluator> evaluators = ImmutableMap.of(
      SecureStoreMacroEvaluator.FUNCTION_NAME, new SecureStoreMacroEvaluator(namespace, getContext()),
      OAuthMacroEvaluator.FUNCTION_NAME, new OAuthMacroEvaluator(getContext()),
      ConnectionMacroEvaluator.FUNCTION_NAME, new ConnectionMacroEvaluator(namespace, getContext())
    );
    MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(new BasicArguments(arguments), evaluators,
                                                              DefaultMacroEvaluator.MAP_FUNCTIONS);
    MacroParserOptions macroParserOptions = MacroParserOptions.builder()
      .skipInvalidMacros()
      .setEscaping(false)
      .setFunctionWhitelist(evaluators.keySet())
      .build();
    Function<Map<String, String>, Map<String, String>> macroFn =
      macroProperties -> getContext().evaluateMacros(namespace, macroProperties, macroEvaluator, macroParserOptions);
    String validationResponse = GSON.toJson(ValidationUtils.validate(
      namespace, validationRequest, getContext().createServicePluginConfigurer(namespace), macroFn));
    responder.sendString(validationResponse);
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

    if (StudioUtil.isBatchPipeline(artifactSummary)) {
      responder.sendJson(validateBatchPipeline(appRequest));
    } else if (StudioUtil.isStreamingPipeline(artifactSummary)) {
      responder.sendJson(validateStreamingPipeline(appRequest));
    } else {
      responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST,
                          String.format("Invalid artifact '%s'. Must be '%s' or '%s'.", artifactSummary.getName(),
                                        StudioUtil.ARTIFACT_BATCH_NAME, StudioUtil.ARTIFACT_STREAMING_NAME));
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
