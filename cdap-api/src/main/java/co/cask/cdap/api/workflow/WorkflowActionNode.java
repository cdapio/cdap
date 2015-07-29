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

package co.cask.cdap.api.workflow;

import co.cask.cdap.api.schedule.SchedulableProgramType;

import java.util.Set;
import javax.annotation.Nullable;

/**
 * Represents the ACTION node in the {@link Workflow}.
 */
public class WorkflowActionNode extends WorkflowNode {
  private final ScheduleProgramInfo program;
  private final WorkflowActionSpecification actionSpecification;

  /**
   * Constructs the ACTION node in the {@link Workflow} for MapReduce or Spark program.
   * @param nodeId the id with which the node is to be created
   * @param program the program that will be executed at the node
   */
  public WorkflowActionNode(String nodeId, ScheduleProgramInfo program) {
    this(nodeId, program, null);
  }

  /**
   * Constructs the ACTION node in the {@link Workflow} for MapReduce or Spark program.
   * @param nodeId the id with which the node is to be created
   * @param program the program that will be executed at the node
   * @param parentNodeIds the set of parent node ids
   */
  public WorkflowActionNode(String nodeId, ScheduleProgramInfo program, Set<String> parentNodeIds) {
    super(nodeId, WorkflowNodeType.ACTION, parentNodeIds);
    this.program = program;
    this.actionSpecification = null;
  }

  /**
   * Constructs the ACTION node in the {@link Workflow} for custom action.
   * @param nodeId the id with which the node is to be created
   * @param actionSpecification the specification of the custom action
   */
  public WorkflowActionNode(String nodeId, WorkflowActionSpecification actionSpecification) {
    this(nodeId, actionSpecification, null);
  }

  /**
   * Constructs the ACTION node in the {@link Workflow} for custom action.
   * @param nodeId the id with which the node is to be created
   * @param actionSpecification the specification of the custom action
   * @param parentNodeIds the set of parent node ids
   */
  public WorkflowActionNode(String nodeId, WorkflowActionSpecification actionSpecification, Set<String> parentNodeIds) {
    super(nodeId, WorkflowNodeType.ACTION, parentNodeIds);
    this.program = new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION, actionSpecification.getName());
    this.actionSpecification = actionSpecification;
  }

  /**
   *
   * @return the program information associated with the {@link WorkflowNode}
   */
  public ScheduleProgramInfo getProgram() {
    return program;
  }

  /**
   *
   * @return the {@link WorkflowActionSpecification} for the custom action represented by this {@link WorkflowNode}
   */
  @Nullable
  public WorkflowActionSpecification getActionSpecification() {
    return actionSpecification;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("WorkflowActionNode{");
    sb.append("nodeId=").append(nodeId);
    sb.append(", program=").append(program);
    sb.append(", actionSpecification=").append(actionSpecification);
    sb.append('}');
    return sb.toString();
  }
}
