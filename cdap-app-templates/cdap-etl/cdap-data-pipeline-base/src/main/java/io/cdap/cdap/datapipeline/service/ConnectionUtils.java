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

import com.google.common.base.Strings;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.service.http.ServicePluginConfigurer;
import io.cdap.cdap.datapipeline.connection.DefaultConnectorContext;
import io.cdap.cdap.datapipeline.connection.LimitingConnector;
import io.cdap.cdap.etl.api.batch.BatchConnector;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.DirectConnector;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.connection.ConnectionBadRequestException;
import io.cdap.cdap.etl.proto.connection.ConnectorDetail;
import io.cdap.cdap.etl.proto.connection.PluginDetail;
import io.cdap.cdap.etl.proto.connection.PluginInfo;
import io.cdap.cdap.etl.proto.connection.SampleResponse;
import io.cdap.cdap.etl.proto.validation.SimpleFailureCollector;
import io.cdap.cdap.etl.spec.TrackedPluginSelector;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;

public final class ConnectionUtils {

  private ConnectionUtils() {
    // prevent instantiation of util class.
  }

  /**
   * Returns {@link Connector}
   * @return {@link Connector}
   */
  public static Connector getConnector(ServicePluginConfigurer configurer, PluginInfo pluginInfo,
                                       TrackedPluginSelector pluginSelector, MacroEvaluator macroEvaluator,
                                       MacroParserOptions options) {
    Connector connector = null;
    try {
      connector = configurer.usePlugin(pluginInfo.getType(), pluginInfo.getName(), UUID.randomUUID().toString(),
                                       PluginProperties.builder().addAll(pluginInfo.getProperties()).build(),
                                       pluginSelector, macroEvaluator, options);
    } catch (InvalidPluginConfigException e) {
      throw new ConnectionBadRequestException(
        String.format("Unable to instantiate connector plugin: %s", e.getMessage()), e);
    }

    if (connector == null) {
      throw new ConnectionBadRequestException(String.format("Unable to find connector '%s'", pluginInfo.getName()));
    }
    return connector;
  }

  /**
   * Return {@link SampleResponse} for the connector
   *
   * @throws IOException
   */
  public static SampleResponse getSampleResponse(Connector connector, ConnectorContext connectorContext,
                                                 SampleRequest sampleRequest, ConnectorDetail detail,
                                                 ServicePluginConfigurer pluginConfigurer) throws IOException {
    if (connector instanceof DirectConnector) {
      DirectConnector directConnector = (DirectConnector) connector;
      List<StructuredRecord> sample = directConnector.sample(connectorContext, sampleRequest);
      return new SampleResponse(detail, sample.isEmpty() ? null : sample.get(0).getSchema(), sample);
    }
    if (connector instanceof BatchConnector) {
      LimitingConnector limitingConnector = new LimitingConnector((BatchConnector) connector, pluginConfigurer);
      List<StructuredRecord> sample = limitingConnector.sample(connectorContext, sampleRequest);
      return new SampleResponse(detail, sample.isEmpty() ? null : sample.get(0).getSchema(), sample);
    }
    throw new ConnectionBadRequestException("Connector is not supported. "
                                    + "The supported connector should be DirectConnector or BatchConnector.");
  }

  /**
   * Returns {@link ConnectorDetail} with all plugins
   *
   * @return {@link ConnectorDetail}
   */
  public static ConnectorDetail getConnectorDetail(ArtifactId artifactId, ConnectorSpec spec) {
    ArtifactSelectorConfig artifact = new ArtifactSelectorConfig(artifactId.getScope().name(),
                                                                 artifactId.getName(),
                                                                 artifactId.getVersion().getVersion());
    Set<PluginDetail> relatedPlugins = new HashSet<>();
    spec.getRelatedPlugins().forEach(pluginSpec -> relatedPlugins.add(
      new PluginDetail(pluginSpec.getName(), pluginSpec.getType(), pluginSpec.getProperties(), artifact,
                       spec.getSchema())));
    return new ConnectorDetail(relatedPlugins, spec.getSupportedSampleTypes());
  }

  /**
   * Returns a {@link ConnectorContext}
   * @return {@link ConnectorContext}
   */
  public static ConnectorContext getConnectorContext(ServicePluginConfigurer pluginConfigurer) {
    SimpleFailureCollector failureCollector = new SimpleFailureCollector();
    return new DefaultConnectorContext(failureCollector, pluginConfigurer);
  }

  /**
   * Returns {@link ConnectorSpec}
   *
   * @return filtered {@link ConnectorSpec} on the basis of plugin name and type
   */
  public static ConnectorSpec filterSpecWithPluginNameAndType(ConnectorSpec spec, @Nullable String pluginName,
                                                              @Nullable String pluginType) {
    if (Strings.isNullOrEmpty(pluginName) && Strings.isNullOrEmpty(pluginType)) {
      return spec;
    }
    ConnectorSpec.Builder specBuilder = ConnectorSpec.builder();
    specBuilder.setSchema(spec.getSchema());
    for (PluginSpec pluginSpec : spec.getRelatedPlugins()) {
      if (Strings.isNullOrEmpty(pluginName)) {
        if (pluginType.equalsIgnoreCase(pluginSpec.getType())) {
          specBuilder.addRelatedPlugin(pluginSpec);
        }
        continue;
      }

      if (Strings.isNullOrEmpty(pluginType)) {
        if (pluginName.equalsIgnoreCase(pluginSpec.getName())) {
          specBuilder.addRelatedPlugin(pluginSpec);
        }
        continue;
      }

      if (pluginType.equalsIgnoreCase(pluginSpec.getType()) && pluginName.equalsIgnoreCase(pluginSpec.getName())) {
        specBuilder.addRelatedPlugin(pluginSpec);
      }
    }
    return specBuilder.build();
  }
}
