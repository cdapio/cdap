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
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.service.http.ServicePluginConfigurer;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.SystemAppTaskContext;
import io.cdap.cdap.datapipeline.connection.DefaultConnectorConfigurer;
import io.cdap.cdap.datapipeline.connection.DefaultConnectorContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorConfigurer;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.common.ArtifactSelectorProvider;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.OAuthMacroEvaluator;
import io.cdap.cdap.etl.common.SecureStoreMacroEvaluator;
import io.cdap.cdap.etl.proto.connection.Connection;
import io.cdap.cdap.etl.proto.connection.ConnectionCreationRequest;
import io.cdap.cdap.etl.proto.connection.PluginInfo;
import io.cdap.cdap.etl.proto.validation.SimpleFailureCollector;
import io.cdap.cdap.etl.spec.TrackedPluginSelector;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

/**
 * {@link RunnableTask} for executing connection creation/test remotely
 */
public class RemoteConnectionTestTask extends RemoteConnectionTaskBase {

  private static final Gson GSON = new Gson();

  @Override
  protected void execute(RunnableTaskContext context, SystemAppTaskContext systemAppContext) throws Exception {
    //De serialize all requests
    RemoteConnectionRequest remoteConnectionTestRequest =
      GSON.fromJson(context.getParam(),
                    RemoteConnectionRequest.class);

    String namespace = remoteConnectionTestRequest.getNamespace();

    ConnectionCreationRequest connectionCreationRequest =
      GSON.fromJson(remoteConnectionTestRequest.getRequest(), ConnectionCreationRequest.class);

    ServicePluginConfigurer pluginConfigurer = systemAppContext.createServicePluginConfigurer(namespace);
    ConnectorConfigurer connectorConfigurer = new DefaultConnectorConfigurer(pluginConfigurer);
    SimpleFailureCollector failureCollector = new SimpleFailureCollector();
    ConnectorContext connectorContext = new DefaultConnectorContext(failureCollector, pluginConfigurer);
    TrackedPluginSelector pluginSelector = new TrackedPluginSelector(
      new ArtifactSelectorProvider().getPluginSelector(connectionCreationRequest.getPlugin().getArtifact()));
    try (Connector connector = getConnector(systemAppContext, pluginConfigurer, connectionCreationRequest.getPlugin(),
                                            namespace, pluginSelector)) {
      connector.configure(connectorConfigurer);
      try {
        connector.test(connectorContext);
        failureCollector.getOrThrowException();
      } catch (ValidationException e) {
        context.writeResult(GSON.toJson(e.getFailures()).getBytes(StandardCharsets.UTF_8));
        return;
      }
    }
  }
}
