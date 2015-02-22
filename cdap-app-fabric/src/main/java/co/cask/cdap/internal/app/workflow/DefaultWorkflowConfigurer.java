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

import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowAction;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.api.workflow.WorkflowConfigurer;
import co.cask.cdap.api.workflow.WorkflowForkBranch;
import co.cask.cdap.api.workflow.WorkflowForkConfigurer;
import co.cask.cdap.api.workflow.WorkflowForkNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.internal.workflow.DefaultWorkflowActionSpecification;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link WorkflowConfigurer}.
 */
public class DefaultWorkflowConfigurer implements WorkflowConfigurer {

  private final String className;
  private String name;
  private String description;
  private Map<String, String> properties;
  private WorkflowNodeIdProvider nodeIdProvider;

  private final List<WorkflowNode> nodes = Lists.newArrayList();
  private final Map<String, WorkflowActionSpecification> customActionMap = Maps.newHashMap();

  public DefaultWorkflowConfigurer(Workflow workflow) {
    this.className = workflow.getClass().getName();
    this.name = workflow.getClass().getSimpleName();
    this.description = "";
    nodeIdProvider = new WorkflowNodeIdProvider();
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

  WorkflowNode getWorkflowActionNode(String programName, SchedulableProgramType programType) {
    switch (programType) {
      case MAPREDUCE:
        Preconditions.checkNotNull(programName, "MapReduce name is null.");
        Preconditions.checkArgument(!programName.isEmpty(), "MapReduce name is empty.");
        break;
      case SPARK:
        Preconditions.checkNotNull(programName, "Spark name is null.");
        Preconditions.checkArgument(!programName.isEmpty(), "Spark name is empty.");
        break;
      case CUSTOM_ACTION:
        //no-op
        break;
      default:
        break;
    }
    String nodeId = nodeIdProvider.getUniqueNodeId();
    return new WorkflowActionNode(nodeId, new ScheduleProgramInfo(programType, programName));
  }

  WorkflowNode getWorkflowCustomActionNode(WorkflowAction action) {
    Preconditions.checkArgument(action != null, "WorkflowAction is null.");
    WorkflowActionSpecification spec = new DefaultWorkflowActionSpecification(action);
    customActionMap.put(spec.getName(), spec);
    return getWorkflowActionNode(spec.getName(), SchedulableProgramType.CUSTOM_ACTION);
  }

  void addWorkflowForkNode(String forkNodeId, List<WorkflowForkBranch> branches) {
    nodes.add(new WorkflowForkNode(forkNodeId, branches));
  }

  @Override
  public void addMapReduce(String mapReduce) {
    nodes.add(getWorkflowActionNode(mapReduce, SchedulableProgramType.MAPREDUCE));
  }

  @Override
  public void addSpark(String spark) {
    nodes.add(getWorkflowActionNode(spark, SchedulableProgramType.SPARK));
  }

  @Override
  public void addAction(WorkflowAction action) {
    nodes.add(getWorkflowCustomActionNode(action));
  }

  public WorkflowNodeIdProvider getNodeIdProvider() {
    return nodeIdProvider;
  }

  @Override
  public WorkflowForkConfigurer<Void> fork() {
    String forkNodeId = nodeIdProvider.getUniqueNodeId();
    return new DefaultWorkflowForkConfigurer<Void>(this, null, forkNodeId);
  }

  public WorkflowSpecification createSpecification() {
    return new WorkflowSpecification(className, name, description, properties, nodes, customActionMap);
  }
}
