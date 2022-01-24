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
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.SystemAppTaskContext;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.ConnectionMacroEvaluator;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.OAuthMacroEvaluator;
import io.cdap.cdap.etl.common.SecureStoreMacroEvaluator;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.proto.v2.validation.StageValidationRequest;
import io.cdap.cdap.etl.proto.v2.validation.StageValidationResponse;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

/**
 * Task for executing validation requests remotely
 */
public class RemoteValidationTask implements RunnableTask {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .serializeNulls()
    .create();
  private static final Logger LOG = LoggerFactory.getLogger(RemoteValidationTask.class);
  static final String NAMESPACE_DOES_NOT_EXIST = "Namespace '%s' does not exist";

  @Override
  public void run(RunnableTaskContext context) throws Exception {
    SystemAppTaskContext systemAppContext = context.getRunnableTaskSystemAppContext();
    RemoteValidationRequest remoteValidationRequest = GSON.fromJson(context.getParam(), RemoteValidationRequest.class);
    String namespace = remoteValidationRequest.getNamespace();
    String originalRequest = remoteValidationRequest.getRequest();
    StageValidationRequest validationRequest;
    try {
      validationRequest = GSON.fromJson(originalRequest,
                                        StageValidationRequest.class);
      validationRequest.validate();
    } catch (JsonSyntaxException e) {
      throw new IllegalArgumentException(String.format("Unable to decode request body %s", originalRequest), e);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid stage config", e);
    }


    Map<String, String> arguments = Collections.emptyMap();
    // Fetch preferences for this instance and namespace and use them as program arguments if the user selects
    // this option.
    if (validationRequest.getResolveMacrosFromPreferences()) {
      try {
        arguments = systemAppContext.getPreferencesForNamespace(namespace, true);
      } catch (IllegalArgumentException iae) {
        // If this method returns IllegalArgumentException, it means the namespace doesn't exist.
        // If this is the case, we return a 404 error.
        throw new IllegalArgumentException(String.format(NAMESPACE_DOES_NOT_EXIST, namespace), iae);
      }
    }

    Map<String, MacroEvaluator> evaluators = ImmutableMap.of(
      SecureStoreMacroEvaluator.FUNCTION_NAME,
      new SecureStoreMacroEvaluator(namespace, systemAppContext),
      OAuthMacroEvaluator.FUNCTION_NAME, new OAuthMacroEvaluator(systemAppContext),
      ConnectionMacroEvaluator.FUNCTION_NAME,
      new ConnectionMacroEvaluator(namespace, systemAppContext)
    );
    MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(new BasicArguments(arguments), evaluators,
                                                              DefaultMacroEvaluator.MAP_FUNCTIONS);
    MacroParserOptions macroParserOptions = MacroParserOptions.builder()
      .skipInvalidMacros()
      .setEscaping(false)
      .setFunctionWhitelist(evaluators.keySet())
      .build();
    Function<Map<String, String>, Map<String, String>> macroFn =
      macroProperties -> systemAppContext
        .evaluateMacros(namespace, macroProperties, macroEvaluator, macroParserOptions);
    PluginConfigurer pluginConfigurer = systemAppContext.createPluginConfigurer(namespace);
    StageValidationResponse validationResponse = ValidationUtils.validate(namespace, validationRequest,
                                                                          pluginConfigurer, macroFn);

    // If the validation success and if it only involves system artifacts, then we don't need to restart task runner
    if (validationResponse.getFailures().isEmpty()) {
      StageSpec spec = validationResponse.getSpec();
      if (spec != null) {
        context.setTerminateOnComplete(!ArtifactScope.SYSTEM.equals(spec.getPlugin().getArtifact().getScope()));
      }
    }
    context.writeResult(GSON.toJson(validationResponse).getBytes());
  }
}
