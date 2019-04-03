/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.id.ProgramRunId;

/**
 *
 */
public interface WorkflowStateWriter {

  /**
   * Sets the {@link WorkflowToken} for a specified run of a workflow.
   *
   * @param workflowRunId workflow run for which the {@link WorkflowToken} is to be set
   * @param token the {@link WorkflowToken} to set to
   */
  void setWorkflowToken(ProgramRunId workflowRunId, WorkflowToken token);

  /**
   * Add node state for the given {@link Workflow} run. This method is used to update the
   * state of the custom actions started by Workflow.
   *
   * @param workflowRunId the Workflow run
   * @param nodeStateDetail the node state to be added for the Workflow run
   */
  void addWorkflowNodeState(ProgramRunId workflowRunId, WorkflowNodeStateDetail nodeStateDetail);
}
