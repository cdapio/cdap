/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.service.http.HttpServiceConfigurer;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceHandlerSpecification;
import co.cask.cdap.api.service.http.ServiceHttpEndpoint;
import co.cask.cdap.internal.app.DefaultPluginConfigurer;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.internal.specification.DataSetFieldExtractor;
import co.cask.cdap.internal.specification.PropertyFieldExtractor;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of {@link HttpServiceConfigurer}.
 */
public class DefaultHttpServiceHandlerConfigurer extends DefaultPluginConfigurer implements HttpServiceConfigurer {

  private final HttpServiceHandler handler;
  private final String name;
  private Map<String, String> properties;
  private Set<String> datasets;

  /**
   * Instantiates the class with the given {@link HttpServiceHandler}.
   * The properties and description are set to empty values and the name is the handler class name.
   *
   * @param handler the handler for the service
   */
  public DefaultHttpServiceHandlerConfigurer(HttpServiceHandler handler,
                                             NamespaceId deployNamespace,
                                             ArtifactId artifactId,
                                             ArtifactRepository artifactRepository,
                                             PluginInstantiator pluginInstantiator) {
    super(deployNamespace, artifactId, artifactRepository, pluginInstantiator);
    this.handler = handler;
    this.name = handler.getClass().getSimpleName();
    this.properties = new HashMap<>();
    this.datasets = new HashSet<>();
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
}
