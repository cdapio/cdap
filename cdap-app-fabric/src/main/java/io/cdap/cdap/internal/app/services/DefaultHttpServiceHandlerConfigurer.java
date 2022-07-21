/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services;

import io.cdap.cdap.api.SystemTableConfigurer;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.api.service.http.HttpServiceConfigurer;
import io.cdap.cdap.api.service.http.HttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceHandlerSpecification;
import io.cdap.cdap.api.service.http.ServiceHttpEndpoint;
import io.cdap.cdap.api.service.http.SystemHttpServiceConfigurer;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.AbstractConfigurer;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentRuntimeInfo;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.internal.lang.Reflections;
import io.cdap.cdap.internal.specification.DataSetFieldExtractor;
import io.cdap.cdap.internal.specification.PropertyFieldExtractor;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link HttpServiceConfigurer}.
 */
public class DefaultHttpServiceHandlerConfigurer extends AbstractConfigurer implements SystemHttpServiceConfigurer {

  private final HttpServiceHandler handler;
  private final String name;
  private final SystemTableConfigurer systemTableConfigurer;
  private Map<String, String> properties;
  private Set<String> datasets;

  /**
   * Instantiates the class with the given {@link HttpServiceHandler}.
   * The properties and description are set to empty values and the name is the handler class name.
   *
   * @param handler the handler for the service
   * @param pluginFinder
   */
  public DefaultHttpServiceHandlerConfigurer(HttpServiceHandler handler,
                                             Id.Namespace deployNamespace,
                                             Id.Artifact artifactId,
                                             PluginFinder pluginFinder,
                                             PluginInstantiator pluginInstantiator,
                                             SystemTableConfigurer systemTableConfigurer,
                                             @Nullable AppDeploymentRuntimeInfo runtimeInfo,
                                             FeatureFlagsProvider featureFlagsProvider) {
    super(deployNamespace, artifactId, pluginFinder, pluginInstantiator, runtimeInfo, featureFlagsProvider);
    this.handler = handler;
    this.name = handler.getClass().getSimpleName();
    this.properties = new HashMap<>();
    this.datasets = new HashSet<>();
    this.systemTableConfigurer = systemTableConfigurer;
  }

  /**
   * Sets the runtime properties.
   *
   * @param properties the HTTP Service runtime properties
   */
  @Override
  public void setProperties(Map<String, String> properties) {
    this.properties = new HashMap<>(properties);
  }

  /**
   * Creates a {@link HttpServiceHandlerSpecification} from the parameters stored in this class.
   *
   * @return a new specification from the parameters stored in this instance
   */
  public HttpServiceHandlerSpecification createSpecification() {
    List<ServiceHttpEndpoint> endpoints = new ArrayList<>();
    // Inspect the handler to grab all @UseDataset, @Property and endpoints.
    Reflections.visit(handler, handler.getClass(),
                      new DataSetFieldExtractor(datasets),
                      new PropertyFieldExtractor(properties),
                      new ServiceEndpointExtractor(endpoints));
    return new HttpServiceHandlerSpecification(handler.getClass().getName(), name, "", properties, datasets, endpoints);
  }


  @Override
  public void createTable(StructuredTableSpecification tableSpecification) {
    systemTableConfigurer.createTable(tableSpecification);
  }
}
