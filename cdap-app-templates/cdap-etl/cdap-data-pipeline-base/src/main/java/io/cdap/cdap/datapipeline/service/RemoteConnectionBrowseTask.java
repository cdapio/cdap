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
import io.cdap.cdap.datapipeline.connection.DefaultConnectorConfigurer;
import io.cdap.cdap.datapipeline.connection.DefaultConnectorContext;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.common.ArtifactSelectorProvider;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.OAuthMacroEvaluator;
import io.cdap.cdap.etl.common.SecureStoreMacroEvaluator;
import io.cdap.cdap.etl.proto.connection.Connection;
import io.cdap.cdap.etl.proto.connection.PluginInfo;
import io.cdap.cdap.etl.proto.validation.SimpleFailureCollector;
import io.cdap.cdap.etl.spec.TrackedPluginSelector;

import java.util.Collections;
import java.util.Map;

/**
 * {@link RunnableTask} for remote connection browsing
 */
public class RemoteConnectionBrowseTask implements RunnableTask {

  private static final Gson GSON = new Gson();

  @Override
  public void run(RunnableTaskContext context) throws Exception {
    //Get SystemAppTaskContext
    SystemAppTaskContext systemAppContext = context.getRunnableTaskSystemAppContext();
    //De serialize all requests
    RemoteConnectionBrowseRequest connectionBrowseRequest = GSON.fromJson(context.getParam(),
                                                                          RemoteConnectionBrowseRequest.class);
    String namespace = connectionBrowseRequest.getNamespace();
    BrowseRequest browseRequest = GSON.fromJson(connectionBrowseRequest.getBrowseRequest(), BrowseRequest.class);
    Connection connection = connectionBrowseRequest.getConnection();

    //Plugin selector and configurer
    TrackedPluginSelector pluginSelector = new TrackedPluginSelector(
      new ArtifactSelectorProvider().getPluginSelector(connection.getPlugin().getArtifact()));
    ServicePluginConfigurer servicePluginConfigurer = systemAppContext.createServicePluginConfigurer(namespace);
    try (Connector connector = getConnector(systemAppContext, servicePluginConfigurer, connection.getPlugin(),
                                            namespace, pluginSelector)) {
    //configure and browse
      connector.configure(new DefaultConnectorConfigurer(servicePluginConfigurer));
      ConnectorContext connectorContext = new DefaultConnectorContext(new SimpleFailureCollector(),
                                                                      servicePluginConfigurer);
      BrowseDetail browseDetail = connector.browse(connectorContext, browseRequest);
      //serialize the result as json and write the bytes
      context.writeResult(GSON.toJson(browseDetail).getBytes());
    }
  }

  private Connector getConnector(SystemAppTaskContext systemAppContext, ServicePluginConfigurer configurer,
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
