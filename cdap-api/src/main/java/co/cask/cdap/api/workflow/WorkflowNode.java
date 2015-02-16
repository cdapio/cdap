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
  private final String nodeName;
  private final WorkflowNodeType nodeType;

  public WorkflowNode(String nodeName, WorkflowNodeType nodeType) {
    this.nodeName = nodeName;
    this.nodeType = nodeType;
  }

  /**
   * If the {@link WorkflowNodeType} of the {@link WorkflowNode} is {@link WorkflowNodeType.ACTION},
   * the name of the node is same as the name of the program or custom action represented by the node
   * @return the name of the {@link WorkflowNode}
   */
  public String getName() {
    return nodeName;
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

    return nodeName.equals(that.nodeName) && nodeType == that.nodeType;
  }

  @Override
  public int hashCode() {
    int result = nodeName.hashCode();
    result = 31 * result + nodeType.hashCode();
    return result;
  }
}
