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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Represents node in the {@link Workflow}.
 */
public abstract class WorkflowNode {
  protected final String nodeId;
  protected final WorkflowNodeType nodeType;
  protected final Set<String> parentNodeIds;

  public WorkflowNode(String nodeId, WorkflowNodeType nodeType, @Nullable Set<String> parentNodeIds) {
    this.nodeId = nodeId;
    this.nodeType = nodeType;
    this.parentNodeIds = parentNodeIds == null ? Collections.unmodifiableSet(new HashSet<String>())
      : Collections.unmodifiableSet(new HashSet<>(parentNodeIds));
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

  /**
   *
   * @return the set of parent node ids.
   */
  public Set<String> getParentNodeIds() {
    return parentNodeIds;
  }
}
