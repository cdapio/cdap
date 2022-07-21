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

import com.google.gson.Gson;
import io.cdap.cdap.api.service.http.ServicePluginConfigurer;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.SystemAppTaskContext;
import io.cdap.cdap.datapipeline.connection.DefaultConnectorConfigurer;
import io.cdap.cdap.datapipeline.connection.DefaultConnectorContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorConfigurer;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.common.ArtifactSelectorProvider;
import io.cdap.cdap.etl.proto.connection.ConnectionCreationRequest;
import io.cdap.cdap.etl.proto.validation.SimpleFailureCollector;
import io.cdap.cdap.etl.spec.TrackedPluginSelector;

/**
 * {@link RunnableTask} for executing connection creation/test remotely
 */
public class RemoteConnectionTestTask extends RemoteConnectionTaskBase {

  private static final Gson GSON = new Gson();

  @Override
  public String execute(SystemAppTaskContext systemAppContext, RemoteConnectionRequest request) throws Exception {
    String namespace = request.getNamespace();
    ConnectionCreationRequest connectionCreationRequest = GSON.fromJson(request.getRequest(),
                                                                        ConnectionCreationRequest.class);
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
        return "";
      } catch (ValidationException e) {
        return GSON.toJson(e.getFailures());
      }
    }
  }
}
