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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowAction;
import co.cask.cdap.api.workflow.WorkflowConditionConfigurer;
import co.cask.cdap.api.workflow.WorkflowConditionNode;
import co.cask.cdap.api.workflow.WorkflowConfigurer;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowForkConfigurer;
import co.cask.cdap.api.workflow.WorkflowForkNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * Default implementation of {@link WorkflowConfigurer}.
 */
public class DefaultWorkflowConfigurer implements WorkflowConfigurer, WorkflowForkJoiner, WorkflowConditionAdder {

  private final String className;
  private String name;
  private String description;
  private Map<String, String> properties;
  private int nodeIdentifier = 0;

  private final List<WorkflowNode> nodes = Lists.newArrayList();

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
    nodes.add(WorkflowNodeCreator.createWorkflowActionNode(mapReduce, SchedulableProgramType.MAPREDUCE));
  }

  @Override
  public void addSpark(String spark) {
    nodes.add(WorkflowNodeCreator.createWorkflowActionNode(spark, SchedulableProgramType.SPARK));
  }

  @Override
  public void addAction(WorkflowAction action) {
    nodes.add(WorkflowNodeCreator.createWorkflowCustomActionNode(action));
  }

  @Override
  public WorkflowForkConfigurer<? extends WorkflowConfigurer> fork() {
    return new DefaultWorkflowForkConfigurer<>(this);
  }

  @Override
  public WorkflowConditionConfigurer<? extends WorkflowConfigurer> condition(Predicate<WorkflowContext> predicate) {
    return new DefaultWorkflowConditionConfigurer<>(predicate.getClass().getSimpleName(), this,
                                                    predicate.getClass().getName());
  }

  public WorkflowSpecification createSpecification() {
    return new WorkflowSpecification(className, name, description, properties, createNodesWithId(nodes));
  }

  private List<WorkflowNode> createNodesWithId(List<WorkflowNode> nodes) {
    List<WorkflowNode> nodesWithId = Lists.newArrayList();
    for (WorkflowNode node : nodes) {
      nodesWithId.add(createNodeWithId(node));
    }

    return nodesWithId;
  }

  private WorkflowNode createNodeWithId(WorkflowNode node) {
    WorkflowNode nodeWithId = null;
    switch (node.getType()) {
      case ACTION:
        return node;
      case FORK:
        nodeWithId = createForkNodeWithId(node);
        break;
      case CONDITION:
        nodeWithId = createConditionNodeWithId(node);
        break;
      default:
        break;
    }
    return nodeWithId;
  }

  private WorkflowNode createForkNodeWithId(WorkflowNode node) {
    String forkNodeId = Integer.toString(nodeIdentifier++);
    List<List<WorkflowNode>> branches = Lists.newArrayList();
    WorkflowForkNode forkNode = (WorkflowForkNode) node;

    for (List<WorkflowNode> branch : forkNode.getBranches()) {
      branches.add(createNodesWithId(branch));
    }
    return new WorkflowForkNode(forkNodeId, branches);
  }

  private WorkflowNode createConditionNodeWithId(WorkflowNode node) {
    String conditionNodeId = node.getNodeId();
    WorkflowConditionNode conditionNode = (WorkflowConditionNode) node;
    List<WorkflowNode> ifbranch = Lists.newArrayList();
    List<WorkflowNode> elsebranch = Lists.newArrayList();
    ifbranch.addAll(createNodesWithId(conditionNode.getIfBranch()));
    elsebranch.addAll(createNodesWithId(conditionNode.getElseBranch()));

    return new WorkflowConditionNode(conditionNodeId, conditionNode.getPredicateClassName(), ifbranch, elsebranch);
  }

  @Override
  public void addWorkflowForkNode(List<List<WorkflowNode>> branches) {
    nodes.add(new WorkflowForkNode(null, branches));
  }

  @Override
  public void addWorkflowConditionNode(String conditionNodeName, String predicateClassName, List<WorkflowNode> ifBranch,
                                       List<WorkflowNode> elseBranch) {
    nodes.add(new WorkflowConditionNode(conditionNodeName, predicateClassName, ifBranch, elseBranch));
  }
}
