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

package co.cask.cdap.internal.app.store.remote;

import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.store.RuntimeStore;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.internal.remote.RemoteOpsClient;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of RuntimeStore, which uses an HTTP Client to execute the actual store operations in a remote
 * server.
 */
public class RemoteRuntimeStore extends RemoteOpsClient implements RuntimeStore {

  @Inject
  RemoteRuntimeStore(CConfiguration cConf, DiscoveryServiceClient discoveryClient) {
    super(cConf, discoveryClient);
  }

  @Override
  public void compareAndSetStatus(Id.Program id, String pid, ProgramRunStatus expectedStatus,
                                  ProgramRunStatus newStatus) {
    executeRequest("compareAndSetStatus", id, pid, expectedStatus, newStatus);
  }

  @Override
  public void setStart(Id.Program id, String pid, long startTime, @Nullable String twillRunId,
                       Map<String, String> runtimeArgs, Map<String, String> systemArgs) {
    executeRequest("setStart", id, pid, startTime, twillRunId, runtimeArgs, systemArgs);
  }

  @Override
  public void setStop(Id.Program id, String pid, long endTime, ProgramRunStatus runStatus) {
    // delegates on client side; so corresponding method is not required to be implemented on server side
    setStop(id, pid, endTime, runStatus, null);
  }

  @Override
  public void setStop(Id.Program id, String pid, long endTime, ProgramRunStatus runStatus,
                      @Nullable BasicThrowable failureCause) {
    executeRequest("setStop", id, pid, endTime, runStatus, failureCause);
  }

  @Override
  public void setSuspend(Id.Program id, String pid) {
    executeRequest("setSuspend", id, pid);
  }

  @Override
  public void setResume(Id.Program id, String pid) {
    executeRequest("setResume", id, pid);
  }

  @Override
  public void updateWorkflowToken(ProgramRunId workflowRunId, WorkflowToken token) {
    executeRequest("updateWorkflowToken", workflowRunId, token);
  }

  @Override
  public void addWorkflowNodeState(ProgramRunId workflowRunId, WorkflowNodeStateDetail nodeStateDetail) {
    executeRequest("addWorkflowNodeState", workflowRunId, nodeStateDetail);
  }
}
