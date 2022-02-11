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
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.service.http.ServicePluginConfigurer;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.SystemAppTaskContext;
import io.cdap.cdap.datapipeline.connection.DefaultConnectorConfigurer;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorConfigurer;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.common.ArtifactSelectorProvider;
import io.cdap.cdap.etl.proto.connection.Connection;
import io.cdap.cdap.etl.proto.connection.SpecGenerationRequest;
import io.cdap.cdap.etl.spec.TrackedPluginSelector;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;

/**
 * {@link RunnableTask} for executing connection spec generation remotely
 */
public class RemoteConnectionSpecTask extends RemoteConnectionTaskBase {
  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .setPrettyPrinting().create();

  @Override
  public String execute(SystemAppTaskContext systemAppContext, RemoteConnectionRequest request) throws Exception {
    String namespace = request.getNamespace();
    Connection connection = request.getConnection();
    //Plugin selector and configurer
    TrackedPluginSelector pluginSelector = new TrackedPluginSelector(
      new ArtifactSelectorProvider().getPluginSelector(connection.getPlugin().getArtifact()));
    ServicePluginConfigurer pluginConfigurer = systemAppContext.createServicePluginConfigurer(namespace);

    ConnectorConfigurer connectorConfigurer = new DefaultConnectorConfigurer(pluginConfigurer);
    ConnectorContext connectorContext = ConnectionUtils.getConnectorContext(pluginConfigurer);
    SpecGenerationRequest specRequest = GSON.fromJson(request.getRequest(), SpecGenerationRequest.class);
    try (Connector connector = getConnector(systemAppContext, pluginConfigurer, connection.getPlugin(), namespace,
                                            pluginSelector)) {
      connector.configure(connectorConfigurer);
      ConnectorSpecRequest connectorSpecRequest = ConnectorSpecRequest.builder().setPath(specRequest.getPath())
        .setConnection(connection.getName())
        .setProperties(specRequest.getProperties()).build();
      ConnectorSpec spec = connector.generateSpec(connectorContext, connectorSpecRequest);
      ConnectorSpec newSpec = ConnectionUtils.filterSpecWithPluginNameAndType(spec, specRequest.getPluginName(),
                                                                              specRequest.getPluginType());
      return GSON.toJson(ConnectionUtils.getConnectorDetail(pluginSelector.getSelectedArtifact(), newSpec));
    }
  }
}
