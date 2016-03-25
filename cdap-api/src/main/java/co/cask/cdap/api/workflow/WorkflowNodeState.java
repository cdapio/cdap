/*
 * Copyright © 2016 Cask Data, Inc.
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

import javax.annotation.Nullable;

/**
 * Class to represent the state of the node in the {@link Workflow}.
 */
public final class WorkflowNodeState {

  /**
   * Status of the node running inside the Workflow.
   */
  public enum NodeStatus {
    RUNNING,
    COMPLETED,
    FAILED,
    KILLED
  }

  private final String nodeId;
  private final NodeStatus nodeStatus;
  private final String runId;
  private final String failureCause;

  /**
   * Create a new instance.
   * @param nodeId id of the node inside the Workflow
   * @param nodeStatus status of the node
   * @param runId run id assigned to the node, null if current node represents custom action or predicate
   * @param failureCause cause of failure, null if execution of the node succeeded
   */
  public WorkflowNodeState(String nodeId, NodeStatus nodeStatus, @Nullable String runId,
                           @Nullable String failureCause) {
    this.nodeId = nodeId;
    this.nodeStatus = nodeStatus;
    this.runId = runId;
    this.failureCause = failureCause;
  }

  /**
   * Return the id of the node for which the state is maintained.
   */
  public String getNodeId() {
    return nodeId;
  }

  /**
   * Return the status of the node for which the state is maintained.
   */
  public NodeStatus getNodeStatus() {
    return nodeStatus;
  }

  /**
   * Return the run id if node represents programs, such as MapReduce or Spark.
   * For custom actions and predicates we do not currently have run id, so method returns {@code null}.
   */
  @Nullable
  public String getRunId() {
    return runId;
  }

  /**
   * Return the detail message string for failure if node execution failed, otherwise {@code null} is returned.
   */
  @Nullable
  public String getFailureCause() {
    return failureCause;
  }
}
