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
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.service.http.ServicePluginConfigurer;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.SystemAppTaskContext;
import io.cdap.cdap.datapipeline.connection.DefaultConnectorConfigurer;
import io.cdap.cdap.datapipeline.connection.DefaultConnectorContext;
import io.cdap.cdap.datapipeline.connection.LimitingConnector;
import io.cdap.cdap.etl.api.batch.BatchConnector;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorConfigurer;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.DirectConnector;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.common.ArtifactSelectorProvider;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.OAuthMacroEvaluator;
import io.cdap.cdap.etl.common.SecureStoreMacroEvaluator;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.connection.Connection;
import io.cdap.cdap.etl.proto.connection.ConnectorDetail;
import io.cdap.cdap.etl.proto.connection.PluginDetail;
import io.cdap.cdap.etl.proto.connection.PluginInfo;
import io.cdap.cdap.etl.proto.connection.SampleResponse;
import io.cdap.cdap.etl.proto.connection.SampleResponseCodec;
import io.cdap.cdap.etl.proto.validation.SimpleFailureCollector;
import io.cdap.cdap.etl.spec.TrackedPluginSelector;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.BadRequestException;

/**
 * {@link RunnableTask} for executing connection data sampling remotely
 */
public class RemoteConnectionSampleTask extends RemoteConnectionTaskBase {

  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .registerTypeAdapter(SampleResponse.class, new SampleResponseCodec()).setPrettyPrinting().create();

  @Override
  public void execute(RunnableTaskContext context, SystemAppTaskContext systemAppContext) throws Exception {
    //De serialize all requests
    RemoteConnectionRequest remoteConnectionSampleTask = GSON.fromJson(context.getParam(),
                                                                       RemoteConnectionRequest.class);
    String namespace = remoteConnectionSampleTask.getNamespace();
    SampleRequest sampleRequest = GSON.fromJson(remoteConnectionSampleTask.getRequest(), SampleRequest.class);
    Connection connection = remoteConnectionSampleTask.getConnection();
    String connectionName = connection.getName();

    ServicePluginConfigurer pluginConfigurer = systemAppContext.createServicePluginConfigurer(namespace);
    ConnectorConfigurer connectorConfigurer = new DefaultConnectorConfigurer(pluginConfigurer);
    SimpleFailureCollector failureCollector = new SimpleFailureCollector();
    ConnectorContext connectorContext = new DefaultConnectorContext(failureCollector, pluginConfigurer);
    TrackedPluginSelector pluginSelector = new TrackedPluginSelector(
      new ArtifactSelectorProvider().getPluginSelector(connection.getPlugin().getArtifact()));

    try (Connector connector = getConnector(systemAppContext, pluginConfigurer, connection.getPlugin(),
                                            namespace, pluginSelector)) {
      connector.configure(new DefaultConnectorConfigurer(pluginConfigurer));
      ConnectorSpecRequest specRequest = ConnectorSpecRequest.builder().setPath(sampleRequest.getPath())
        .setConnection(connectionName)
        .setProperties(sampleRequest.getProperties()).build();
      ConnectorSpec spec = connector.generateSpec(connectorContext, specRequest);
      ConnectorDetail detail = getConnectorDetail(pluginSelector.getSelectedArtifact(), spec);
      SampleResponse sampleResponse = ConnectionUtils.getSampleResponse(connector, connectorContext, sampleRequest,
                                                                        detail, pluginConfigurer);
      context.writeResult(GSON.toJson(sampleResponse).getBytes(StandardCharsets.UTF_8));
    }
  }
}
