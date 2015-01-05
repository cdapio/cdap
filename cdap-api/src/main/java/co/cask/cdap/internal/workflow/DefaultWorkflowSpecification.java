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
package co.cask.cdap.internal.workflow;

import co.cask.cdap.api.workflow.WorkflowActionEntry;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 *
 */
public final class DefaultWorkflowSpecification implements WorkflowSpecification {


  private final String className;
  private final String name;
  private final String description;
  private final Map<String, WorkflowActionSpecification> customActionMap;
  private final List<WorkflowActionEntry> actions;
  private final List<String> schedules;
  private final Map<String, String> properties;

  public DefaultWorkflowSpecification(String className, String name, String description,
                                      Map<String, String> properties,
                                      List<WorkflowActionEntry> actions,
                                      Map<String, WorkflowActionSpecification> customActionMap,
                                      List<String> schedules) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.properties = properties == null ? ImmutableMap.<String, String>of() : ImmutableMap.copyOf(properties);
    this.actions = ImmutableList.copyOf(actions);
    this.schedules = ImmutableList.copyOf(schedules);
    this.customActionMap = ImmutableMap.copyOf(customActionMap);
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
  public List<WorkflowActionEntry> getActions() {
    return ImmutableList.copyOf(actions);
  }

  @Override
  public List<String> getSchedules() {
    return schedules;
  }

  @Override
  public Map<String, WorkflowActionSpecification> getCustomActionMap() {
    return ImmutableMap.copyOf(customActionMap);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(WorkflowSpecification.class)
      .add("className", className)
      .add("name", name)
      .add("description", description)
      .add("customActionMap", customActionMap)
      .add("actions", actions)
      .add("schedules", schedules)
      .add("properties", properties)
      .toString();
  }
}
