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

package co.cask.cdap.app.store;

import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.id.ProgramRunId;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * A store for runtime information of a {@link Program}.
 */
public interface RuntimeStore {

  /**
   * Compare and set operation that allow to compare and set expected and update status.
   * Implementation of this method should guarantee that the operation is atomic or in transaction.
   *
   * @param id id of the program
   * @param pid the run id
   * @param expectedStatus the expected value
   * @param newStatus the new value
   */
  void compareAndSetStatus(Id.Program id, String pid, ProgramRunStatus expectedStatus, ProgramRunStatus newStatus);

  /**
   * Logs start of program run.
   *
   * @param id id of the program
   * @param pid run id
   * @param startTime start timestamp in seconds; if run id is time-based pass the time from the run id
   * @param twillRunId Twill run id
   * @param runtimeArgs the runtime arguments for this program run
   * @param systemArgs the system arguments for this program run
   */
  void setStart(Id.Program id, String pid, long startTime, @Nullable String twillRunId,
                Map<String, String> runtimeArgs, Map<String, String> systemArgs);

  /**
   * Logs end of program run.
   *
   * @param id id of the program
   * @param pid run id
   * @param endTime end timestamp in seconds
   * @param runStatus {@link ProgramRunStatus} of program run
   */
  void setStop(Id.Program id, String pid, long endTime, ProgramRunStatus runStatus);

  /**
   * Logs end of program run.
   *
   * @param id id of the program
   * @param pid run id
   * @param endTime end timestamp in seconds
   * @param runStatus {@link ProgramRunStatus} of program run
   * @param failureCause failure cause if the program failed to execute
   */
  void setStop(Id.Program id, String pid, long endTime, ProgramRunStatus runStatus,
               @Nullable BasicThrowable failureCause);

  /**
   * Logs suspend of a program run.
   *
   * @param id id of the program
   * @param pid run id
   */
  void setSuspend(Id.Program id, String pid);

  /**
   * Logs resume of a program run.
   *
   * @param id id of the program
   * @param pid run id
   */
  void setResume(Id.Program id, String pid);

  /**
   * Updates the {@link WorkflowToken} for a specified run of a workflow.
   *
   * @param workflowRunId workflow run for which the {@link WorkflowToken} is to be updated
   * @param token the {@link WorkflowToken} to update
   */
  void updateWorkflowToken(ProgramRunId workflowRunId, WorkflowToken token);

  /**
   * Add node state for the given {@link Workflow} run. This method is used to update the
   * state of the custom actions started by Workflow.
   *
   * @param workflowRunId the Workflow run
   * @param nodeStateDetail the node state to be added for the Workflow run
   */
  void addWorkflowNodeState(ProgramRunId workflowRunId, WorkflowNodeStateDetail nodeStateDetail);
}
