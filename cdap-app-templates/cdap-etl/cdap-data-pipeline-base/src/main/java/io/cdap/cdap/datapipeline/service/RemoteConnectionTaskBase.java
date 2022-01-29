/*
 * Copyright © 2022 Cask Data, Inc.
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
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.service.http.ServicePluginConfigurer;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.SystemAppTaskContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.common.ArtifactSelectorProvider;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.OAuthMacroEvaluator;
import io.cdap.cdap.etl.common.SecureStoreMacroEvaluator;
import io.cdap.cdap.etl.proto.connection.Connection;
import io.cdap.cdap.etl.proto.connection.PluginInfo;
import io.cdap.cdap.etl.spec.TrackedPluginSelector;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

/**
 * Base class for {@link RunnableTask} for connection handling
 */
public abstract class RemoteConnectionTaskBase implements RunnableTask {

  private static final Gson GSON = new Gson();

  @Override
  public void run(RunnableTaskContext context) throws Exception {
    //Get SystemAppTaskContext
    SystemAppTaskContext systemAppContext = context.getRunnableTaskSystemAppContext();
    //De serialize all requests
    RemoteConnectionRequest connectionRequest = GSON.fromJson(context.getParam(),
                                                              RemoteConnectionRequest.class);
    String namespace = connectionRequest.getNamespace();
    Connection connection = connectionRequest.getConnection();

    //Plugin selector and configurer
    TrackedPluginSelector pluginSelector = new TrackedPluginSelector(
      new ArtifactSelectorProvider().getPluginSelector(connection.getPlugin().getArtifact()));
    ServicePluginConfigurer servicePluginConfigurer = systemAppContext.createServicePluginConfigurer(namespace);
    //serialize the result as json and write the bytes
    String executedResult = execute(systemAppContext, connection, servicePluginConfigurer, pluginSelector, namespace,
                                    connectionRequest.getRequest());
    context.writeResult(executedResult.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * execute method for specific tasks to implement
   *
   * @param systemAppContext        {@link SystemAppTaskContext}
   * @param connection              {@link Connection}
   * @param servicePluginConfigurer {@link ServicePluginConfigurer}
   * @param pluginSelector          {@link TrackedPluginSelector}
   * @param namespace               namespace string
   * @param request                 request string
   * @return executed response as string
   * @throws Exception
   */
  public abstract String execute(SystemAppTaskContext systemAppContext, Connection connection,
                                 ServicePluginConfigurer servicePluginConfigurer, TrackedPluginSelector pluginSelector,
                                 String namespace, String request) throws Exception;

  /**
   * Returns {@link Connector} after evaluating macros
   *
   * @param systemAppContext {@link SystemAppTaskContext}
   * @param configurer       {@link ServicePluginConfigurer}
   * @param pluginInfo       {@link PluginInfo}
   * @param namespace        namespace string
   * @param pluginSelector   {@link TrackedPluginSelector}
   * @return {@link Connector}
   * @throws Exception
   */
  protected Connector getConnector(SystemAppTaskContext systemAppContext, ServicePluginConfigurer configurer,
                                   PluginInfo pluginInfo, String namespace,
                                   TrackedPluginSelector pluginSelector) throws Exception {

    Map<String, String> arguments = systemAppContext.getPreferencesForNamespace(namespace, true);
    Map<String, MacroEvaluator> evaluators = ImmutableMap.of(
      SecureStoreMacroEvaluator.FUNCTION_NAME, new SecureStoreMacroEvaluator(namespace, systemAppContext),
      OAuthMacroEvaluator.FUNCTION_NAME, new OAuthMacroEvaluator(systemAppContext)
    );
    MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(new BasicArguments(arguments), evaluators,
                                                              Collections.singleton(OAuthMacroEvaluator.FUNCTION_NAME));
    MacroParserOptions options = MacroParserOptions.builder()
      .skipInvalidMacros()
      .setEscaping(false)
      .setFunctionWhitelist(evaluators.keySet())
      .build();
    return ConnectionUtils.getConnector(configurer, pluginInfo, pluginSelector, macroEvaluator, options);
  }
}
