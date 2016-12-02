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

package co.cask.cdap.gateway.handlers.meta;

import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.internal.remote.MethodArgument;
import co.cask.cdap.internal.app.store.remote.RemoteRuntimeStore;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.Iterator;
import java.util.Map;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * The {@link HttpHandler} for handling REST calls from a {@link RemoteRuntimeStore}.
 */
@Path(AbstractRemoteSystemOpsHandler.VERSION + "/execute")
public class RemoteRuntimeStoreHandler extends AbstractRemoteSystemOpsHandler {

  private final Store store;

  @Inject
  RemoteRuntimeStoreHandler(Store store) {
    this.store = store;
  }

  @POST
  @Path("/setStart")
  public void setStart(HttpRequest request, HttpResponder responder) throws Exception {
    Iterator<MethodArgument> arguments = parseArguments(request);

    ProgramId program = deserializeNext(arguments);
    String pid = deserializeNext(arguments);
    long startTime = deserializeNext(arguments);
    String twillRunId = deserializeNext(arguments);
    Map<String, String> runtimeArgs = deserializeNext(arguments);
    Map<String, String> systemArgs = deserializeNext(arguments);
    store.setStart(program, pid, startTime, twillRunId, runtimeArgs, systemArgs);

    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/setStop")
  public void setStop(HttpRequest request, HttpResponder responder) throws Exception {
    Iterator<MethodArgument> arguments = parseArguments(request);

    ProgramId program = deserializeNext(arguments);
    String pid = deserializeNext(arguments);
    long endTime = deserializeNext(arguments);
    ProgramRunStatus runStatus = deserializeNext(arguments);
    BasicThrowable failureCause = deserializeNext(arguments);
    store.setStop(program, pid, endTime, runStatus, failureCause);

    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/setSuspend")
  public void setSuspend(HttpRequest request, HttpResponder responder) throws Exception {
    Iterator<MethodArgument> arguments = parseArguments(request);

    ProgramId program = deserializeNext(arguments);
    String pid = deserializeNext(arguments);
    store.setSuspend(program, pid);

    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/setResume")
  public void setResume(HttpRequest request, HttpResponder responder) throws Exception {
    Iterator<MethodArgument> arguments = parseArguments(request);

    ProgramId program = deserializeNext(arguments);
    String pid = deserializeNext(arguments);
    store.setResume(program, pid);

    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/updateWorkflowToken")
  public void updateWorkflowToken(HttpRequest request, HttpResponder responder) throws Exception {
    Iterator<MethodArgument> arguments = parseArguments(request);

    ProgramRunId workflowRunId = deserializeNext(arguments);
    WorkflowToken token = deserializeNext(arguments);
    store.updateWorkflowToken(workflowRunId, token);

    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/addWorkflowNodeState")
  public void addWorkflowNodeState(HttpRequest request, HttpResponder responder) throws Exception {
    Iterator<MethodArgument> arguments = parseArguments(request);

    ProgramRunId workflowRunId = deserializeNext(arguments);
    WorkflowNodeStateDetail nodeStateDetail = deserializeNext(arguments);
    store.addWorkflowNodeState(workflowRunId, nodeStateDetail);

    responder.sendStatus(HttpResponseStatus.OK);
  }
}
