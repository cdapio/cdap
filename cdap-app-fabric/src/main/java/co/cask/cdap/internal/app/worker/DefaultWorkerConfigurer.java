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

package co.cask.cdap.internal.app.worker;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.worker.Worker;
import co.cask.cdap.api.worker.WorkerConfigurer;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.internal.specification.PropertyFieldExtractor;
import com.clearspring.analytics.util.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import java.util.Map;
import java.util.Set;

/**
 * Default implementation of the {@link WorkerConfigurer}.
 */
public class DefaultWorkerConfigurer implements WorkerConfigurer {

  private final String className;
  private final Map<String, String> propertyFields;

  private String name;
  private String description;
  private Resources resource;
  private int instances;
  private Map<String, String> properties;
  private Set<String> datasets;

  public DefaultWorkerConfigurer(Worker worker) {
    this.name = worker.getClass().getSimpleName();
    this.className = worker.getClass().getName();
    this.propertyFields = Maps.newHashMap();
    this.description = "";
    this.resource = new Resources();
    this.instances = 1;
    this.properties = ImmutableMap.of();
    this.datasets = Sets.newHashSet();

    // Grab all @Property fields
    Reflections.visit(worker, TypeToken.of(worker.getClass()), new PropertyFieldExtractor(propertyFields));
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public void setResources(Resources resources) {
    Preconditions.checkArgument(resources != null, "Resources cannot be null.");
    this.resource = resources;
  }

  @Override
  public void setInstances(int instances) {
    Preconditions.checkArgument(instances > 0, "Instances must be > 0.");
    this.instances = instances;
  }

  @Override
  public void setProperties(Map<String, String> properties) {
    this.properties = ImmutableMap.copyOf(properties);
  }

  @Override
  public void useDatasets(Iterable<String> datasets) {
    Iterables.addAll(this.datasets, datasets);
  }

  public WorkerSpecification createSpecification() {
    Map<String, String> properties = Maps.newHashMap(this.properties);
    properties.putAll(propertyFields);
    return new WorkerSpecification(className, name, description, properties, datasets, resource, instances);
  }
}
