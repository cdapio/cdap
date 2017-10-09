/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.test.internal;

import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.internal.AppFabricClient;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ScheduleDetail;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.WorkflowTokenDetail;
import co.cask.cdap.proto.WorkflowTokenNodeDetail;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.test.AbstractProgramManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ScheduleManager;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.base.Throwables;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A default implementation of {@link FlowManager}.
 */
public class DefaultWorkflowManager extends AbstractProgramManager<WorkflowManager> implements WorkflowManager {
  private final Id.Program programId;
  private final AppFabricClient appFabricClient;

  public DefaultWorkflowManager(Id.Program programId, AppFabricClient appFabricClient,
                                DefaultApplicationManager applicationManager) {
    super(programId, applicationManager);
    this.programId = programId;
    this.appFabricClient = appFabricClient;
  }

  @Override
  public List<ScheduleDetail> getProgramSchedules() {
    try {
      return appFabricClient.getProgramSchedules(
        programId.getNamespaceId(), programId.getApplicationId(), programId.getId());
    } catch (NotFoundException e) {
      // this can only happen if the workflow was deleted, unlikely during a test but if so, empty list is correct
      return Collections.emptyList();
    }
  }

  @Override
  public WorkflowTokenDetail getToken(String runId, @Nullable WorkflowToken.Scope scope,
                                      @Nullable String key) throws NotFoundException {
    return appFabricClient.getWorkflowToken(programId.getNamespaceId(), programId.getApplicationId(), programId.getId(),
                                            runId, scope, key);
  }

  @Override
  public WorkflowTokenNodeDetail getTokenAtNode(String runId, String nodeName, @Nullable WorkflowToken.Scope scope,
                                                @Nullable String key) throws NotFoundException {
    return appFabricClient.getWorkflowToken(programId.getNamespaceId(), programId.getApplicationId(), programId.getId(),
                                            runId, nodeName, scope, key);
  }

  @Override
  public Map<String, WorkflowNodeStateDetail> getWorkflowNodeStates(String workflowRunId)
    throws NotFoundException {
    return appFabricClient.getWorkflowNodeStates(
      new ProgramRunId(programId.getNamespaceId(), programId.getApplicationId(), programId.getType(),
                       programId.getId(), workflowRunId));
  }

  public ScheduleManager getSchedule(final String schedName) {

    return new ScheduleManager() {
      @Override
      public void suspend() {
        try {
          appFabricClient.suspend(programId.getNamespaceId(), programId.getApplicationId(), schedName);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public void resume() {
        try {
          appFabricClient.resume(programId.getNamespaceId(), programId.getApplicationId(), schedName);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public String status(int expectedCode) {
        try {
          return appFabricClient.scheduleStatus(programId.getNamespaceId(), programId.getApplicationId(),
                                                schedName, expectedCode);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }
}
