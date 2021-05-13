/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.test.internal;

import com.google.common.base.Throwables;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.AppFabricClient;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.WorkflowNodeStateDetail;
import io.cdap.cdap.proto.WorkflowTokenDetail;
import io.cdap.cdap.proto.WorkflowTokenNodeDetail;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.spi.AccessException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.test.AbstractProgramManager;
import io.cdap.cdap.test.ScheduleManager;
import io.cdap.cdap.test.WorkflowManager;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A default implementation of {@link WorkflowManager}.
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
  public List<ScheduleDetail> getProgramSchedules() throws UnauthorizedException {
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
                                      @Nullable String key) throws NotFoundException, UnauthorizedException {
    return appFabricClient.getWorkflowToken(programId.getNamespaceId(), programId.getApplicationId(), programId.getId(),
                                            runId, scope, key);
  }

  @Override
  public WorkflowTokenNodeDetail getTokenAtNode(String runId, String nodeName, @Nullable WorkflowToken.Scope scope,
                                                @Nullable String key) throws NotFoundException, UnauthorizedException {
    return appFabricClient.getWorkflowToken(programId.getNamespaceId(), programId.getApplicationId(), programId.getId(),
                                            runId, nodeName, scope, key);
  }

  @Override
  public Map<String, WorkflowNodeStateDetail> getWorkflowNodeStates(String workflowRunId)
    throws NotFoundException, UnauthorizedException {
    return appFabricClient.getWorkflowNodeStates(
      new ProgramRunId(programId.getNamespaceId(), programId.getApplicationId(), programId.getType(),
                       programId.getId(), workflowRunId));
  }

  @Override
  public ScheduleManager getSchedule(final String schedName) {

    return new ScheduleManager() {
      @Override
      public void suspend() throws AccessException {
        try {
          appFabricClient.suspend(programId.getNamespaceId(), programId.getApplicationId(), schedName);
        } catch (AccessException e) {
          throw e;
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public void resume() throws AccessException {
        try {
          appFabricClient.resume(programId.getNamespaceId(), programId.getApplicationId(), schedName);
        } catch (AccessException e) {
          throw e;
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public String status(int expectedCode) throws AccessException {
        try {
          return appFabricClient.scheduleStatus(programId.getNamespaceId(), programId.getApplicationId(),
                                                schedName, expectedCode);
        } catch (AccessException e) {
          throw e;
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }
}
