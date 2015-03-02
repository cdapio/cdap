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

import java.util.List;

/**
 * Represents the FORK node in the {@link Workflow}
 */
public class WorkflowForkNode extends WorkflowNode {
  private final List<List<WorkflowNode>> branches;

  public WorkflowForkNode(String nodeId, List<List<WorkflowNode>> branches) {
    super(nodeId, WorkflowNodeType.FORK);
    this.branches = branches;
  }

  public List<List<WorkflowNode>> getBranches() {
    return branches;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("WorkflowForkNode{");
    sb.append("nodeId=").append(nodeId);
    sb.append(", branches=").append(branches);
    sb.append('}');
    return sb.toString();
  }
}
