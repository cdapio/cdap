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
 * Represents node in the {@link Workflow}.
 */
public abstract class WorkflowNode {
  protected final String nodeId;
  protected final WorkflowNodeType nodeType;

  public WorkflowNode(String nodeId, WorkflowNodeType nodeType) {
    this.nodeId = nodeId;
    this.nodeType = nodeType;
  }

  /**
   *
   * @return the id of the {@link WorkflowNode}
   */
  public String getNodeId() {
    return nodeId;
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

    return nodeId.equals(that.nodeId) && nodeType == that.nodeType;
  }

  @Override
  public int hashCode() {
    int result = nodeId.hashCode();
    result = 31 * result + nodeType.hashCode();
    return result;
  }
}
