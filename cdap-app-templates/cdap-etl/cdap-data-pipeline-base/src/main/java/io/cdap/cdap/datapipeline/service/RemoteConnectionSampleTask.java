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
 * {@link RunnableTask} for remote connection browsing
 */
public class RemoteConnectionSampleTask implements RunnableTask {

  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .registerTypeAdapter(SampleResponse.class, new SampleResponseCodec()).setPrettyPrinting().create();

  @Override
  public void run(RunnableTaskContext context) throws Exception {
    System.out.println("wyzhang: RemoteConnectionSampleTask run start");
    //Get SystemAppTaskContext
    SystemAppTaskContext systemAppContext = context.getRunnableTaskSystemAppContext();
    //De serialize all requests
    RemoteConnectionSampleRequest remoteConnectionSampleTask = GSON.fromJson(context.getParam(),
                                                                             RemoteConnectionSampleRequest.class);
    String namespace = remoteConnectionSampleTask.getNamespace();
    SampleRequest sampleRequest = GSON.fromJson(remoteConnectionSampleTask.getSampleRequest(), SampleRequest.class);
    Connection connection = remoteConnectionSampleTask.getConnection();
    String connectionString = connection.getName();

    ServicePluginConfigurer pluginConfigurer = systemAppContext.createServicePluginConfigurer(namespace);
    ConnectorConfigurer connectorConfigurer = new DefaultConnectorConfigurer(pluginConfigurer);
    SimpleFailureCollector failureCollector = new SimpleFailureCollector();
    ConnectorContext connectorContext = new DefaultConnectorContext(failureCollector, pluginConfigurer);
    TrackedPluginSelector pluginSelector = new TrackedPluginSelector(
      new ArtifactSelectorProvider().getPluginSelector(connection.getPlugin().getArtifact()));

    try (Connector connector = getConnector(systemAppContext, pluginConfigurer, connection.getPlugin(),
                                            namespace, pluginSelector)) {
      //configure and browse
      connector.configure(new DefaultConnectorConfigurer(pluginConfigurer));
      ConnectorSpecRequest specRequest = ConnectorSpecRequest.builder().setPath(sampleRequest.getPath())
        .setConnection(connectionString)
        .setProperties(sampleRequest.getProperties()).build();
      ConnectorSpec spec = connector.generateSpec(connectorContext, specRequest);
      ConnectorDetail detail = getConnectorDetail(pluginSelector.getSelectedArtifact(), spec);
      System.out.println("wyzhang: RemoteConnectionSampleTask run here 1");
      if (connector instanceof DirectConnector) {
        DirectConnector directConnector = (DirectConnector) connector;
        List<StructuredRecord> sample = directConnector.sample(connectorContext, sampleRequest);
        System.out.println("wyzhang: sample len = " + sample.size());
        System.out.println("wyzhang: sample  = " + GSON.toJson(
          new SampleResponse(detail, sample.isEmpty() ? null : sample.get(0).getSchema(),
                             sample)).getBytes(StandardCharsets.UTF_8));
        context.writeResult(GSON.toJson(
          new SampleResponse(detail, sample.isEmpty() ? null : sample.get(0).getSchema(),
                             sample)).getBytes(StandardCharsets.UTF_8));
        return;
      }
      System.out.println("wyzhang: RemoteConnectionSampleTask run here 2");
      if (connector instanceof BatchConnector) {
        LimitingConnector limitingConnector = new LimitingConnector((BatchConnector) connector, pluginConfigurer);
        List<StructuredRecord> sample = limitingConnector.sample(connectorContext, sampleRequest);
        System.out.println("wyzhang: sample len = " + sample.size());
        System.out.println("wyzhang: sample  = " +
                             GSON.toJson(new SampleResponse(detail, sample.isEmpty() ? null : sample.get(0).getSchema(),
                                                           sample)).getBytes(StandardCharsets.UTF_8));
        context.writeResult(GSON.toJson(new SampleResponse(detail, sample.isEmpty() ? null : sample.get(0).getSchema(),
                                                           sample)).getBytes(StandardCharsets.UTF_8));
        return;
      }
      throw new BadRequestException("Connector is not supported. " +
                                    "The supported connector should be DirectConnector or BatchConnector.");

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

  private ConnectorDetail getConnectorDetail(ArtifactId artifactId, ConnectorSpec spec) {
    ArtifactSelectorConfig artifact = new ArtifactSelectorConfig(artifactId.getScope().name(),
                                                                 artifactId.getName(),
                                                                 artifactId.getVersion().getVersion());
    Set<PluginDetail> relatedPlugins = new HashSet<>();
    spec.getRelatedPlugins().forEach(pluginSpec -> relatedPlugins.add(
      new PluginDetail(pluginSpec.getName(), pluginSpec.getType(), pluginSpec.getProperties(), artifact,
                       spec.getSchema())));
    return new ConnectorDetail(relatedPlugins);
  }
}
