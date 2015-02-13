/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.api.worker;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.common.PropertyProvider;
import co.cask.cdap.api.dataset.Dataset;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Specification for {@link Worker}s.
 */
public final class WorkerSpecification implements ProgramSpecification, PropertyProvider {
  private final String className;
  private final String name;
  private final String description;
  private final Map<String, String> properties;
  private final Set<String> datasets;
  private final Resources resources;
  private final int instances;

  public WorkerSpecification(String className, String name, String description, Map<String, String> properties,
                             Set<String> datasets, Resources resources, int instances) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.properties = Collections.unmodifiableMap(new HashMap<String, String>(properties));
    this.datasets = Collections.unmodifiableSet(new HashSet<String>(datasets));
    this.resources = resources;
    this.instances = instances;
  }

  @Override
  public String getClassName() {
    return className;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public String getProperty(String key) {
    return properties.get(key);
  }

  /**
   * @return resources requirements which will be used to run the {@link Worker}
   */
  public Resources getResources() {
    return resources;
  }

  /**
   * @return an immutable set of {@link Dataset} name that are used by the {@link Worker}
   */
  public Set<String> getDatasets() {
    return datasets;
  }

  /**
   * @return number of instances for the worker
   */
  public int getInstances() {
    return instances;
  }
}
