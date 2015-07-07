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
package co.cask.cdap.internal.workflow;

import co.cask.cdap.api.workflow.WorkflowAction;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.internal.specification.DataSetFieldExtractor;
import co.cask.cdap.internal.specification.PropertyFieldExtractor;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import java.util.Map;
import java.util.Set;

/**
 * The default implementation for a {@link WorkflowActionSpecification}.
 */
public class DefaultWorkflowActionSpecification implements WorkflowActionSpecification {

  private final String className;
  private final String name;
  private final String description;
  private final Map<String, String> properties;
  private final Set<String> datasets;

  public DefaultWorkflowActionSpecification(String name, String description,
                                            Map<String, String> properties, Set<String> datasets) {
    this.className = null;
    this.name = name;
    this.description = description;
    this.properties = ImmutableMap.copyOf(properties);
    this.datasets = ImmutableSet.copyOf(datasets);
  }

  public DefaultWorkflowActionSpecification(WorkflowAction action) {
    WorkflowActionSpecification spec = action.configure();

    Map<String, String> properties = Maps.newHashMap(spec.getProperties());
    Set<String> datasets = Sets.newHashSet();
    Reflections.visit(action, TypeToken.of(action.getClass()),
                      new DataSetFieldExtractor(datasets),
                      new PropertyFieldExtractor(properties));

    // Add datasets that are specified in overriding configure with useDataset method
    datasets.addAll(spec.getDatasets());

    this.className = action.getClass().getName();
    this.name = spec.getName();
    this.description = spec.getDescription();
    this.properties = ImmutableMap.copyOf(properties);
    this.datasets = ImmutableSet.copyOf(datasets);
  }

  public DefaultWorkflowActionSpecification(String className, String name, String description,
                                            Map<String, String> properties, Set<String> datasets) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.properties = ImmutableMap.copyOf(properties);
    this.datasets = ImmutableSet.copyOf(datasets);
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

  @Override
  public String toString() {
    return Objects.toStringHelper(WorkflowActionSpecification.class)
      .add("name", name)
      .add("class", className)
      .add("options", properties)
      .add("datasets", datasets)
      .toString();
  }

  @Override
  public Set<String> getDatasets() {
    return datasets;
  }
}
