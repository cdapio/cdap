/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package io.cdap.cdap.test.remote;

import com.google.common.base.Throwables;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.client.ScheduleClient;
import io.cdap.cdap.client.WorkflowClient;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.util.RESTClient;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.UnauthenticatedException;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.WorkflowNodeStateDetail;
import io.cdap.cdap.proto.WorkflowTokenDetail;
import io.cdap.cdap.proto.WorkflowTokenNodeDetail;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.WorkflowId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.test.AbstractProgramManager;
import io.cdap.cdap.test.ScheduleManager;
import io.cdap.cdap.test.WorkflowManager;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Remote implementation of {@link WorkflowManager}.
 */
public class RemoteWorkflowManager extends AbstractProgramManager<WorkflowManager> implements WorkflowManager {

  private final ScheduleClient scheduleClient;
  private final WorkflowClient workflowClient;
  private final WorkflowId workflowId;

  public RemoteWorkflowManager(WorkflowId programId, ClientConfig clientConfig, RESTClient restClient,
                               RemoteApplicationManager applicationManager) {
    super(programId, applicationManager);
    this.workflowId = programId;
    this.workflowClient = new WorkflowClient(clientConfig, restClient);
    this.scheduleClient = new ScheduleClient(clientConfig, restClient);
  }

  @Override
  public List<ScheduleDetail> getProgramSchedules() {
    try {
      return scheduleClient.listSchedules(workflowId);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public WorkflowTokenDetail getToken(String runId, @Nullable WorkflowToken.Scope scope,
                                      @Nullable String key) throws NotFoundException {
    try {
      return workflowClient.getWorkflowToken(workflowId.run(runId), scope, key);
    } catch (IOException | UnauthenticatedException | UnauthorizedException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public WorkflowTokenNodeDetail getTokenAtNode(String runId, String nodeName, @Nullable WorkflowToken.Scope scope,
                                                @Nullable String key) throws NotFoundException {
    try {
      return workflowClient.getWorkflowTokenAtNode(workflowId.run(runId), nodeName, scope, key);
    } catch (IOException | UnauthenticatedException | UnauthorizedException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Map<String, WorkflowNodeStateDetail> getWorkflowNodeStates(String workflowRunId)
    throws NotFoundException {
    try {
      ProgramRunId programRunId = workflowId.run(workflowRunId);
      return workflowClient.getWorkflowNodeStates(programRunId);
    } catch (IOException | UnauthenticatedException | UnauthorizedException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public ScheduleManager getSchedule(final String schedName) {
    return new ScheduleManager() {
      @Override
      public void suspend() {
        try {
          scheduleClient.suspend(programId.getParent().schedule(schedName));
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public void resume() {
        try {
          scheduleClient.resume(programId.getParent().schedule(schedName));
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public String status(int expectedCode) {
        try {
          return scheduleClient.getStatus(programId.getParent().schedule(schedName));
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }
}
