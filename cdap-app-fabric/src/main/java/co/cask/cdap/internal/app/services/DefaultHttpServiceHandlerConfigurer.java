/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.internal.specification.DataSetFieldExtractor;
import co.cask.cdap.internal.specification.PropertyFieldExtractor;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of {@link HttpServiceConfigurer}.
 */
public class DefaultHttpServiceHandlerConfigurer implements HttpServiceConfigurer {

  private final Map<String, String> propertyFields;
  private final String className;
  private final String name;
  private Map<String, String> properties;
  private Set<String> datasets;
  private final List<ServiceHttpEndpoint> endpoints;

  /**
   * Instantiates the class with the given {@link HttpServiceHandler}.
   * The properties and description are set to empty values and the name is the handler class name.
   *
   * @param handler the handler for the service
   */
  public DefaultHttpServiceHandlerConfigurer(HttpServiceHandler handler) {
    this.propertyFields = Maps.newHashMap();
    this.className = handler.getClass().getName();
    this.name = handler.getClass().getSimpleName();
    this.properties = ImmutableMap.of();
    this.datasets = Sets.newHashSet();
    this.endpoints = Lists.newArrayList();

    // Inspect the handler to grab all @UseDataset, @Property and endpoints.
    Reflections.visit(handler, TypeToken.of(handler.getClass()),
                      new DataSetFieldExtractor(datasets),
                      new PropertyFieldExtractor(propertyFields),
                      new ServiceEndpointExtractor(endpoints));
  }

  /**
   * Sets the runtime properties.
   *
   * @param properties the HTTP Service runtime properties
   */
  @Override
  public void setProperties(Map<String, String> properties) {
    this.properties = ImmutableMap.copyOf(properties);
  }

  @Override
  public void useDatasets(Iterable<String> datasets) {
    Iterables.addAll(this.datasets, datasets);
  }

  /**
   * Creates a {@link HttpServiceHandlerSpecification} from the parameters stored in this class.
   *
   * @return a new specification from the parameters stored in this instance
   */
  public HttpServiceHandlerSpecification createSpecification() {
    Map<String, String> properties = Maps.newHashMap(this.properties);
    properties.putAll(propertyFields);
    return new HttpServiceHandlerSpecification(className, name, "", properties, datasets, endpoints);
  }
}
