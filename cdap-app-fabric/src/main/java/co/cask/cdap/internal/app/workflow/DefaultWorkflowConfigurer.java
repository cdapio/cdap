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

package co.cask.cdap.internal.app.workflow;

import co.cask.cdap.api.schedule.SchedulableProgram;
import co.cask.cdap.api.workflow.ProgramNameTypeInfo;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowAction;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.api.workflow.WorkflowConfigurer;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.internal.workflow.DefaultWorkflowActionSpecification;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * Default implementation of {@link WorkflowConfigurer}.
 */
public class DefaultWorkflowConfigurer implements WorkflowConfigurer {

  private final String className;
  private String name;
  private String description;
  private Map<String, String> properties;

  private final List<ProgramNameTypeInfo> actions = Lists.newArrayList();
  private final Map<String, WorkflowActionSpecification> customActionMap = Maps.newHashMap();

  public DefaultWorkflowConfigurer(Workflow workflow) {
    this.className = workflow.getClass().getName();
    this.name = workflow.getClass().getSimpleName();
    this.description = "";
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
  public void setProperties(Map<String, String> properties) {
    this.properties = ImmutableMap.copyOf(properties);
  }

  @Override
  public void addMapReduce(String mapReduce) {
    actions.add(new ProgramNameTypeInfo
                  (mapReduce, SchedulableProgram.MAPREDUCE));
  }

  @Override
  public void addSpark(String spark) {
    actions.add(new ProgramNameTypeInfo
                  (spark, SchedulableProgram.SPARK));
  }

  @Override
  public void addAction(WorkflowAction action) {
    Preconditions.checkArgument(action != null, "WorkflowAction is null.");
    WorkflowActionSpecification spec = new DefaultWorkflowActionSpecification(action);
    customActionMap.put(spec.getName(), spec);
    actions.add(new ProgramNameTypeInfo(spec.getName(), SchedulableProgram.CUSTOM_ACTION));
  }

  public WorkflowSpecification createSpecification() {
    return new WorkflowSpecification(className, name, description, properties,
                                            actions, customActionMap);
  }
}
