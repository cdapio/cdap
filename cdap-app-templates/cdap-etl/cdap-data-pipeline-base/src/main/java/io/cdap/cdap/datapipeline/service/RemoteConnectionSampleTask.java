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
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.common.ArtifactSelectorProvider;
import io.cdap.cdap.etl.proto.connection.Connection;
import io.cdap.cdap.etl.proto.connection.ConnectorDetail;
import io.cdap.cdap.etl.proto.connection.SampleResponse;
import io.cdap.cdap.etl.proto.connection.SampleResponseCodec;
import io.cdap.cdap.etl.spec.TrackedPluginSelector;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;

/**
 * {@link RunnableTask} for executing connection data sampling remotely
 */
public class RemoteConnectionSampleTask extends RemoteConnectionTaskBase {

  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .registerTypeAdapter(SampleResponse.class, new SampleResponseCodec()).setPrettyPrinting().create();

  @Override
  public String execute(SystemAppTaskContext systemAppContext, RemoteConnectionRequest request) throws Exception {
    String namespace = request.getNamespace();
    Connection connection = request.getConnection();
    //Plugin selector and configurer
    TrackedPluginSelector pluginSelector = new TrackedPluginSelector(
      new ArtifactSelectorProvider().getPluginSelector(connection.getPlugin().getArtifact()));
    ServicePluginConfigurer pluginConfigurer = systemAppContext.createServicePluginConfigurer(namespace);

    ConnectorContext connectorContext = ConnectionUtils.getConnectorContext(pluginConfigurer);
    SampleRequest sampleRequest = GSON.fromJson(request.getRequest(), SampleRequest.class);
    try (Connector connector = getConnector(systemAppContext, pluginConfigurer, connection.getPlugin(),
                                            namespace, pluginSelector)) {
      connector.configure(new DefaultConnectorConfigurer(pluginConfigurer));
      ConnectorSpecRequest specRequest = ConnectorSpecRequest.builder().setPath(sampleRequest.getPath())
        .setConnection(connection.getName())
        .setProperties(sampleRequest.getProperties()).build();
      ConnectorSpec spec = connector.generateSpec(connectorContext, specRequest);
      ConnectorDetail detail = ConnectionUtils.getConnectorDetail(pluginSelector.getSelectedArtifact(), spec);
      SampleResponse sampleResponse = ConnectionUtils.getSampleResponse(connector, connectorContext, sampleRequest,
                                                                        detail, pluginConfigurer);
      return GSON.toJson(sampleResponse);
    }
  }
}
