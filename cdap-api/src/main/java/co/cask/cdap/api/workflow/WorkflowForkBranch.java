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
 * Represents single branch on the {@link WorkflowForkNode}.
 */
public class WorkflowForkBranch {
  private List<WorkflowNode> nodes;

  public WorkflowForkBranch(List<WorkflowNode> nodes) {
    this.nodes = nodes;
  }

  /**
   *
   * @return the list of {@link WorkflowNode} on the {@link WorkflowForkBranch}
   */
  public List<WorkflowNode> getNodes() {
    return nodes;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("WorkflowForkBranch{");
    sb.append("nodes=").append(nodes);
    sb.append('}');
    return sb.toString();
  }
}
