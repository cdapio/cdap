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

package co.cask.cdap.api.service.http;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.common.PropertyProvider;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.retry.RetryPolicy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Specification for a {@link HttpServiceHandler}.
 */
public final class HttpServiceHandlerSpecification implements PropertyProvider, ProgramSpecification {

  private final String className;
  private final String name;
  private final String description;
  private final Map<String, String> properties;
  private final Set<String> datasets;
  private final List<ServiceHttpEndpoint> endpoints;
  private final RetryPolicy remoteRetryPolicy;

  /**
   * Create an instance of {@link HttpServiceHandlerSpecification}.
   */
  public HttpServiceHandlerSpecification(String className, String name,
                                         String description, Map<String, String> properties,
                                         Set<String> datasets, List<ServiceHttpEndpoint> endpoints,
                                         RetryPolicy remoteRetryPolicy) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
    this.datasets = Collections.unmodifiableSet(new HashSet<>(datasets));
    this.endpoints = Collections.unmodifiableList(new ArrayList<>(endpoints));
    this.remoteRetryPolicy = remoteRetryPolicy;
  }

  /**
   * @return the class name
   */
  @Override
  public String getClassName() {
    return className;
  }

  /**
   * @return the name
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * @return the description
   */
  @Override
  public String getDescription() {
    return description;
  }

  /**
   * @return the properties
   */
  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * @param key for getting specific property value
   * @return the property value
   */
  @Override
  public String getProperty(String key) {
    return properties.get(key);
  }

  /**
   * @return An immutable set of {@link Dataset} names that are used by the {@link HttpServiceHandler}.
   */
  public Set<String> getDatasets() {
    return datasets;
  }

  /**
   * @return An immutable set of {@link ServiceHttpEndpoint}s that are exposed by the {@link HttpServiceHandler}.
   */
  public List<ServiceHttpEndpoint> getEndpoints() {
    return endpoints;
  }

  /**
   * @return RetryPolicy for remote calls or {@code null} if the CDAP default should be used.
   */
  @Nullable
  public RetryPolicy getRemoteRetryPolicy() {
    return remoteRetryPolicy;
  }
}
