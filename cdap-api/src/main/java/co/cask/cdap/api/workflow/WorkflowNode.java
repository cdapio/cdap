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

/**
 * Represents type and program associated with the nodes in the {@link Workflow}
 */
public final class WorkflowNode {
  private final WorkflowNodeType nodeType;
  private final ScheduleProgramInfo programInfo;

  public WorkflowNode(WorkflowNodeType nodeType, ScheduleProgramInfo programInfo) {
    this.nodeType = nodeType;
    this.programInfo = programInfo;
  }

  /**
   * Return the name of the {@link WorkflowNode} which is same as the name of the encapsulated program.
   * In the case of Fork node, returns the fork identifier.
   * @return the name of the {@link WorkflowNode}
   */
  public String getName() {
    return programInfo.getProgramName();
  }

  /**
   *
   * @return the program information associated with the {@link WorkflowNode}
   */
  public ScheduleProgramInfo getProgramInfo() {
    return programInfo;
  }

  /**
   *
   * @return the type of the {@link WorkflowNode}
   */
  public WorkflowNodeType getType() {
    return nodeType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    WorkflowNode that = (WorkflowNode) o;

    return programInfo.equals(that.programInfo) && nodeType == that.nodeType;
  }

  @Override
  public int hashCode() {
    int result = programInfo.hashCode();
    result = 31 * result + nodeType.hashCode();
    return result;
  }
}
