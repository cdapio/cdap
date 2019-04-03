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

package co.cask.cdap.etl.api.batch;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.workflow.WorkflowNodeState;
import co.cask.cdap.api.workflow.WorkflowToken;

import java.util.Map;

/**
 * Context for a batch action, giving access to whether the run was successful, the workflow token, and the state
 * of each phase in the workflow. This API is marked as beta and subject to change.
 */
@Beta
public interface BatchActionContext extends BatchRuntimeContext {

  /**
   * @return a {@link WorkflowToken}
   */
  WorkflowToken getToken();

  /**
   * Return an immutable {@link Map} of node ids to {@link WorkflowNodeState}. This can be used
   * to determine the status of all pipeline phases executed in the current run.
   */
  Map<String, WorkflowNodeState> getNodeStates();

  /**
   * Return true if the execution was successful, false otherwise. This method can be
   * used to determine the status of the pipeline run.
   */
  boolean isSuccessful();
}
