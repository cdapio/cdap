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
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowAction;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowConditionConfigurer;
import co.cask.cdap.api.workflow.WorkflowConditionNode;
import co.cask.cdap.api.workflow.WorkflowConfigurer;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowForkConfigurer;
import co.cask.cdap.api.workflow.WorkflowForkNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowNodeType;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

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
    return new WorkflowSpecification(className, name, description, properties, createNodesWithId(nodes, null));
  }

  private List<WorkflowNode> createNodesWithId(List<WorkflowNode> nodes, WorkflowNode previousNode) {
    List<WorkflowNode> nodesWithId = Lists.newArrayList();
    for (WorkflowNode node : nodes) {
      nodesWithId.add(createNodeWithId(node, previousNode));
      previousNode = node;
    }

    return nodesWithId;
  }

  private WorkflowNode createNodeWithId(WorkflowNode currentNode, WorkflowNode previousNode) {
    WorkflowNode nodeWithId = null;
    switch (currentNode.getType()) {
      case ACTION:
        nodeWithId = createActionNodeWithId(currentNode, previousNode);
        break;
      case FORK:
        nodeWithId = createForkNodeWithId(currentNode, previousNode);
        break;
      case CONDITION:
        nodeWithId = createConditionNodeWithId(currentNode, previousNode);
        break;
      default:
        break;
    }
    return nodeWithId;
  }

  private WorkflowNode createActionNodeWithId(WorkflowNode currentNode, WorkflowNode previousNode) {
    WorkflowActionNode actionNode = (WorkflowActionNode) currentNode;
    ScheduleProgramInfo program = actionNode.getProgram();
    Set<String> parentNodeIds = new HashSet<>();
    populateParentNodeIds(previousNode, parentNodeIds, currentNode);
    if (program.getProgramType() == SchedulableProgramType.CUSTOM_ACTION) {
      return new WorkflowActionNode(currentNode.getNodeId(), actionNode.getActionSpecification(), parentNodeIds);
    }
    return new WorkflowActionNode(currentNode.getNodeId(), program, parentNodeIds);
  }

  private WorkflowNode createForkNodeWithId(WorkflowNode currentNode, WorkflowNode previousNode) {
    String forkNodeId = Integer.toString(nodeIdentifier++);
    List<List<WorkflowNode>> branches = Lists.newArrayList();
    WorkflowForkNode forkNode = (WorkflowForkNode) currentNode;

    for (List<WorkflowNode> branch : forkNode.getBranches()) {
      branches.add(createNodesWithId(branch, previousNode));
    }
    return new WorkflowForkNode(forkNodeId, branches);
  }

  private WorkflowNode createConditionNodeWithId(WorkflowNode currentNode, WorkflowNode previousNode) {
    String conditionNodeId = currentNode.getNodeId();
    WorkflowConditionNode conditionNode = (WorkflowConditionNode) currentNode;
    List<WorkflowNode> ifbranch = Lists.newArrayList();
    List<WorkflowNode> elsebranch = Lists.newArrayList();
    ifbranch.addAll(createNodesWithId(conditionNode.getIfBranch(), conditionNode));
    elsebranch.addAll(createNodesWithId(conditionNode.getElseBranch(), conditionNode));

    Set<String> parentNodeIds = new HashSet<>();
    populateParentNodeIds(previousNode, parentNodeIds, currentNode);
    return new WorkflowConditionNode(conditionNodeId, conditionNode.getPredicateClassName(), ifbranch, elsebranch,
                                     parentNodeIds);
  }

  // This method populates the set parentNodeIds with the parent node ids of the current node.
  // If the previous node is of type ACTION, then that node is considered as the parent of the current node.
  // If the previous node if of type FORK, then the last nodes on all the branches of FORK are considered as
  // the parent of the current node.
  // If the previous node is of type CONDITION and if the node is immediate child of the CONDITION, then the
  // CONDITION node is considered to be parent of the current node, otherwise last nodes on the if branch and
  // else branch are considered to be the parent of the current node.
  private void populateParentNodeIds(WorkflowNode previousNodeId, Set<String> parentNodeIds, WorkflowNode currentNode) {
    if (previousNodeId == null) {
      // return in case of first node in the Workflow
      return;
    }

    switch (previousNodeId.getType()) {
      case ACTION:
        parentNodeIds.add(previousNodeId.getNodeId());
        break;
      case FORK:
        WorkflowForkNode forkNode = (WorkflowForkNode) previousNodeId;
        for (List<WorkflowNode> branch : forkNode.getBranches()) {
          if (branch.isEmpty()) {
            continue;
          }
          populateParentNodeIds(branch.get(branch.size() - 1), parentNodeIds, currentNode);
        }
        break;
      case CONDITION:
        WorkflowConditionNode conditionNode = (WorkflowConditionNode) previousNodeId;
        List<WorkflowNode> ifBranch = conditionNode.getIfBranch();
        List<WorkflowNode> elseBranch = conditionNode.getElseBranch();

        if (isImmediateChildOfConditionNode(conditionNode, currentNode)) {
          parentNodeIds.add(conditionNode.getNodeId());
          break;
        }

        if (!ifBranch.isEmpty()) {
          populateParentNodeIds(ifBranch.get(ifBranch.size() - 1), parentNodeIds, currentNode);
        } else {
          // If branch of the condition is empty, so for the node following condition will have condition node
          // also as a parent
          parentNodeIds.add(conditionNode.getNodeId());
        }

        if (!elseBranch.isEmpty()) {
          populateParentNodeIds(elseBranch.get(elseBranch.size() - 1), parentNodeIds, currentNode);
        } else {
          // Else branch of the condition is empty, so for the node following condition will have condition node
          // also as a parent
          parentNodeIds.add(conditionNode.getNodeId());
        }
        break;
      default:
        throw new IllegalStateException("Node type is invalid.");
    }
  }

  // This method determines if the node is immediate child of the CONDITION node.
  // For e.g. consider the following workflow where c1 is the condition node.
  // If branch of the condition node executes a fork with actions a1, a3, and a4
  // in parallel. Else branch of the condition execute a5. Node a6 gets executed after
  // the If/Else branch of the condition finishes the execution.
  //
  //               T |--a1---a2--|
  //            |----|-----a3----|----|
  //      c1----|    |-----a4----|    |---a6
  //            |----------a5---------|
  //               F
  //
  // For nodes a1, a3, 14, and a5 are considered to be immediate children of the c1 and method
  // returns true for them, so that parent set of a1, a3, a4, and a5 will contain only c1.
  // For node a6, even its previous node is c1, since its not the immediate child of c1, method
  // returns false. For a6, the parent node id set would contain a2, a3, a4, and a5 as determined
  // by the populateParentNodeIds method.
  private boolean isImmediateChildOfConditionNode(WorkflowConditionNode conditionNode, WorkflowNode nodeToTest) {
    Queue<List<WorkflowNode>> branchList = new LinkedList<>();
    branchList.add(conditionNode.getIfBranch());
    branchList.add(conditionNode.getElseBranch());

    while (!branchList.isEmpty()) {
      List<WorkflowNode> branch = branchList.poll();
      if (branch.isEmpty()) {
        continue;
      }
      WorkflowNode node = branch.get(0);
      if (node.getType().equals(WorkflowNodeType.FORK)) {
        for (List<WorkflowNode> forkBranch : ((WorkflowForkNode) node).getBranches()) {
          branchList.add(forkBranch);
        }
        continue;
      }

      if (node.getNodeId().equals(nodeToTest.getNodeId())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void addWorkflowForkNode(List<List<WorkflowNode>> branches) {
    nodes.add(new WorkflowForkNode(null, branches));
  }

  @Override
  public void addWorkflowConditionNode(String conditionNodeName, String predicateClassName, List<WorkflowNode> ifBranch,
                                       List<WorkflowNode> elseBranch) {
    nodes.add(new WorkflowConditionNode(conditionNodeName, predicateClassName, ifBranch, elseBranch, null));
  }
}
