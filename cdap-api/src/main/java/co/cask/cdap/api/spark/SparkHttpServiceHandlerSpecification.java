/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.api.spark;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.PropertyProvider;
import co.cask.cdap.api.service.http.ServiceHttpEndpoint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Specification for Spark HTTP service handler.
 */
public final class SparkHttpServiceHandlerSpecification implements PropertyProvider {

  private final String className;
  private final Map<String, String> properties;
  private final Set<String> datasets;
  private final List<ServiceHttpEndpoint> endpoints;

  public SparkHttpServiceHandlerSpecification(String className, Map<String, String> properties,
                                              Set<String> datasets, List<ServiceHttpEndpoint> endpoints) {
    this.className = className;
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
    this.datasets = Collections.unmodifiableSet(new HashSet<>(datasets));
    this.endpoints = Collections.unmodifiableList(new ArrayList<>(endpoints));
  }

  /**
   * @return the fully qualified class name of the handler class.
   */
  public String getClassName() {
    return className;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  @Nullable
  @Override
  public String getProperty(String key) {
    return properties.get(key);
  }

  /**
   * @return a set of dataset names that declared by the {@link UseDataSet} annotation.
   */
  public Set<String> getDatasets() {
    return datasets;
  }

  /**
   * @return a {@link List} of {@link ServiceHttpEndpoint} that the handler exposes.
   */
  public List<ServiceHttpEndpoint> getEndpoints() {
    return endpoints;
  }
}
