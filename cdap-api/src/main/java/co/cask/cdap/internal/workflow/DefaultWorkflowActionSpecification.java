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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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

  /**
   * This constructor does not set the class name, which will cause null pointer exceptions
   * when serializing the action spec. But we have to keep this for backward-compatibility,
   * because {@link co.cask.cdap.api.workflow.WorkflowActionSpecification.Builder} uses it,
   * and it cannot enforce that the class name be set. Therefore, we must fix the specs
   * produced by this constructor using the second constructor. This can go away as soon
   * as we remove the deprecated builder-style configure method from WorkflowAction.
   */
  @Deprecated
  public DefaultWorkflowActionSpecification(String name, String description,
                                            Map<String, String> properties, Set<String> datasets) {
    this(null, name, description, properties, datasets);
  }

  /**
   * Fix a spec created with the first constructor, in builder-style workflow action configuration,
   * by adding the class name of the acton. This can go away as soon
   * as we remove the deprecated builder-style configure method from WorkflowAction.
   */
  @Deprecated
  public DefaultWorkflowActionSpecification(WorkflowActionSpecification spec, WorkflowAction action) {
    this(action.getClass().getName(), spec.getName(), spec.getDescription(), spec.getProperties(), spec.getDatasets());
  }

  /**
   * Constructor be used by WorkflowActionConfigurer during workflow action configuration.
   */
  public DefaultWorkflowActionSpecification(String className, String name, String description,
                                            Map<String, String> properties, Set<String> datasets) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
    this.datasets = Collections.unmodifiableSet(new HashSet<>(datasets));
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
    return "DefaultWorkflowActionSpecification{" +
      "className='" + className + '\'' +
      ", name='" + name + '\'' +
      ", description='" + description + '\'' +
      ", properties=" + properties +
      ", datasets=" + datasets +
      '}';
  }

  @Override
  public Set<String> getDatasets() {
    return datasets;
  }
}
