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
import co.cask.cdap.internal.app.store.remote.MethodArgument;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * The {@link co.cask.http.HttpHandler} for handling REST calls to meta stores.
 */
// we don't share the same version as other handlers, so we can upgrade/iterate faster
@Path("/v1/execute")
public class RemoteRuntimeStoreHandler extends AbstractHttpHandler {

  private static final Gson GSON = new Gson();
  private static final Type METHOD_ARGUMENT_LIST_TYPE = new TypeToken<List<MethodArgument>>() { }.getType();

  private final Store store;

  @Inject
  public RemoteRuntimeStoreHandler(Store store) {
    this.store = store;
  }

  @POST
  @Path("/compareAndSetStatus")
  public void compareAndSetStatus(HttpRequest request, HttpResponder responder) throws ClassNotFoundException {
    List<MethodArgument> arguments = parseArguments(request);

    Id.Program program = deserialize(arguments.get(0));
    String pid = deserialize(arguments.get(1));
    ProgramRunStatus expectedStatus = deserialize(arguments.get(2));
    ProgramRunStatus updateStatus = deserialize(arguments.get(3));
    store.compareAndSetStatus(program, pid, expectedStatus, updateStatus);

    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/setStartWithTwillRunId")
  public void setStartWithTwillRunId(HttpRequest request, HttpResponder responder) throws ClassNotFoundException {
    List<MethodArgument> arguments = parseArguments(request);

    Id.Program program = deserialize(arguments.get(0));
    String pid = deserialize(arguments.get(1));
    long startTime = deserialize(arguments.get(2));
    String twillRunId = deserialize(arguments.get(3));
    Map<String, String> runtimeArgs = deserialize(arguments.get(4));
    Map<String, String> systemArgs = deserialize(arguments.get(5));
    store.setStart(program, pid, startTime, twillRunId, runtimeArgs, systemArgs);

    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/setStart")
  public void setStart(HttpRequest request, HttpResponder responder) throws ClassNotFoundException {
    List<MethodArgument> arguments = parseArguments(request);

    Id.Program program = deserialize(arguments.get(0));
    String pid = deserialize(arguments.get(1));
    long startTime = deserialize(arguments.get(2));
    store.setStart(program, pid, startTime);

    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/setStop")
  public void setStop(HttpRequest request, HttpResponder responder) throws ClassNotFoundException {
    List<MethodArgument> arguments = parseArguments(request);

    Id.Program program = deserialize(arguments.get(0));
    String pid = deserialize(arguments.get(1));
    long endTime = deserialize(arguments.get(2));
    ProgramRunStatus runStatus = deserialize(arguments.get(3));
    BasicThrowable failureCause = deserialize(arguments.get(4));
    store.setStop(program, pid, endTime, runStatus, failureCause);

    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/setSuspend")
  public void setSuspend(HttpRequest request, HttpResponder responder) throws ClassNotFoundException {
    List<MethodArgument> arguments = parseArguments(request);

    Id.Program program = deserialize(arguments.get(0));
    String pid = deserialize(arguments.get(1));
    store.setSuspend(program, pid);

    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/setResume")
  public void setResume(HttpRequest request, HttpResponder responder) throws ClassNotFoundException {
    List<MethodArgument> arguments = parseArguments(request);

    Id.Program program = deserialize(arguments.get(0));
    String pid = deserialize(arguments.get(1));
    store.setResume(program, pid);

    responder.sendStatus(HttpResponseStatus.OK);
  }


  @POST
  @Path("/updateWorkflowToken")
  public void updateWorkflowToken(HttpRequest request, HttpResponder responder) throws ClassNotFoundException {
    List<MethodArgument> arguments = parseArguments(request);

    ProgramRunId workflowRunId = deserialize(arguments.get(0));
    WorkflowToken token = deserialize(arguments.get(1));
    store.updateWorkflowToken(workflowRunId, token);

    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/addWorkflowNodeState")
  public void addWorkflowNodeState(HttpRequest request, HttpResponder responder) throws ClassNotFoundException {
    List<MethodArgument> arguments = parseArguments(request);

    ProgramRunId workflowRunId = deserialize(arguments.get(0));
    WorkflowNodeStateDetail nodeStateDetail = deserialize(arguments.get(1));
    store.addWorkflowNodeState(workflowRunId, nodeStateDetail);

    responder.sendStatus(HttpResponseStatus.OK);
  }

  private List<MethodArgument> parseArguments(HttpRequest request) {
    String body = request.getContent().toString(Charsets.UTF_8);
    return GSON.fromJson(body, METHOD_ARGUMENT_LIST_TYPE);
  }

  @Nullable
  private <T> T deserialize(@Nullable MethodArgument argument) throws ClassNotFoundException {
    if (argument == null) {
      return null;
    }
    JsonElement value = argument.getValue();
    if (value == null) {
      return null;
    }
    return GSON.<T>fromJson(value, Class.forName(argument.getType()));
  }
}
