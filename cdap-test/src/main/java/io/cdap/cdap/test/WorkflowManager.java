/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package io.cdap.cdap.test;

import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.WorkflowNodeStateDetail;
import io.cdap.cdap.proto.WorkflowTokenDetail;
import io.cdap.cdap.proto.WorkflowTokenNodeDetail;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Workflow manager interface for managing the workflow and its schedules
 */
public interface WorkflowManager extends ProgramManager<WorkflowManager> {

  /**
   * Get the list of schedules of the workflow
   *
   * @return List of {@link ScheduleDetail}.
   */
  List<ScheduleDetail> getProgramSchedules() throws UnauthorizedException;

  /**
   * Get the {@link ScheduleManager} instance to manage the schedule
   *
   * @param scheduleId of the workflow to retrieve
   * @return {@link ScheduleManager} instance to manage the schedule identified by scheduleId
   */
  ScheduleManager getSchedule(String scheduleId);

  /**
   * Get the {@link WorkflowTokenDetail} for the specified workflow run.
   *
   * @param runId the specified workflow run id
   * @param scope the {@link WorkflowToken.Scope}. Defaults to {@link WorkflowToken.Scope#USER}
   * @param key the specified key. If null, returns all key-value pairs
   * @return the {@link WorkflowTokenDetail} for the specified workflow run
   */
  WorkflowTokenDetail getToken(String runId, @Nullable WorkflowToken.Scope scope,
                               @Nullable String key) throws NotFoundException, UnauthorizedException;

  /**
   * Returns the {@link WorkflowTokenNodeDetail} for the specified workflow run at the specified node.
   *
   * @param runId the specified workflow run id
   * @param nodeName the specified node name
   * @param scope the {@link WorkflowToken.Scope}. Defaults to {@link WorkflowToken.Scope#USER}
   * @param key the specified key. If null, returns all key-value pairs emitted by the specified node
   * @return the {@link WorkflowTokenNodeDetail} for the specified workflow run at the specified node.
   */
  WorkflowTokenNodeDetail getTokenAtNode(String runId, String nodeName, @Nullable WorkflowToken.Scope scope,
                                         @Nullable String key) throws NotFoundException, UnauthorizedException;

  /**
   * Get node stated for the specified Workflow run.
   *
   * @param workflowRunId the Workflow run for which node states to be returned
   * @return {@link Map} of node name to the {@link WorkflowNodeStateDetail}
   * @throws NotFoundException when the specified Workflow run is not found
   */
  Map<String, WorkflowNodeStateDetail> getWorkflowNodeStates(String workflowRunId)
    throws NotFoundException, UnauthorizedException;
}
