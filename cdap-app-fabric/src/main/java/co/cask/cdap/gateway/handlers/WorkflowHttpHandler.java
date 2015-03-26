/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.internal.app.runtime.schedule.ScheduledRuntime;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.http.HttpResponder;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.RunIds;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Workflow HTTP Handler.
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class WorkflowHttpHandler extends ProgramLifecycleHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramLifecycleHttpHandler.class);

  @Inject
  public WorkflowHttpHandler(Authenticator authenticator, StoreFactory storeFactory, WorkflowClient workflowClient,
                             CConfiguration configuration, ProgramRuntimeService runtimeService,
                             DiscoveryServiceClient discoveryServiceClient, QueueAdmin queueAdmin, Scheduler scheduler,
                             PreferencesStore preferencesStore, NamespacedLocationFactory namespacedLocationFactory) {
    super(authenticator, storeFactory, workflowClient, configuration, runtimeService, discoveryServiceClient,
          queueAdmin, scheduler, preferencesStore, namespacedLocationFactory);
  }

  @POST
  @Path("/apps/{app-id}/workflows/{workflow-name}/{run-id}/suspend")
  public void suspendWorkflowRun(HttpRequest request, final HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId, @PathParam("app-id") String appId,
                                 @PathParam("workflow-name") String workflowName, @PathParam("run-id") String runId) {
    try {
      Id.Program id = Id.Program.from(namespaceId, appId, ProgramType.WORKFLOW, workflowName);
      ProgramRuntimeService.RuntimeInfo runtimeInfo = runtimeService.list(id).get(RunIds.fromString(runId));
      if (runtimeInfo == null) {
        sendInvalidResponse(responder, id);
        return;
      }
      AppFabricServiceStatus status = suspendResumeProgram(runtimeInfo.getController(), "suspend");
      responder.sendString(status.getCode(), status.getMessage());
    }  catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path("/apps/{app-id}/workflows/{workflow-name}/{run-id}/resume")
  public void resumeWorkflowRun(HttpRequest request, final HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId, @PathParam("app-id") String appId,
                                @PathParam("workflow-name") String workflowName, @PathParam("run-id") String runId) {

    try {
      Id.Program id = Id.Program.from(namespaceId, appId, ProgramType.WORKFLOW, workflowName);
      ProgramRuntimeService.RuntimeInfo runtimeInfo = runtimeService.list(id).get(RunIds.fromString(runId));
      if (runtimeInfo == null) {
        sendInvalidResponse(responder, id);
        return;
      }
      AppFabricServiceStatus status = suspendResumeProgram(runtimeInfo.getController(), "resume");
      responder.sendString(status.getCode(), status.getMessage());
    }  catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private void sendInvalidResponse(HttpResponder responder, Id.Program id) {
    try {
      AppFabricServiceStatus status;
      try {
        ProgramStatus programStatus = getProgramStatus(id, ProgramType.WORKFLOW);
        if (programStatus.getStatus().equals(HttpResponseStatus.NOT_FOUND.toString())) {
          status = AppFabricServiceStatus.PROGRAM_NOT_FOUND;
        } else if (ProgramController.State.COMPLETED.toString().equals(programStatus.getStatus())
          || ProgramController.State.KILLED.toString().equals(programStatus.getStatus())
          || ProgramController.State.ERROR.toString().equals(programStatus.getStatus())) {
          status = AppFabricServiceStatus.PROGRAM_ALREADY_STOPPED;
        } else {
          status = AppFabricServiceStatus.RUNTIME_INFO_NOT_FOUND;
        }
      } catch (Exception e) {
        status = AppFabricServiceStatus.INTERNAL_ERROR;
      }
      responder.sendString(status.getCode(), status.getMessage());
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private AppFabricServiceStatus suspendResumeProgram(ProgramController controller, String action) {
    try {
      if (action.equals("suspend")) {
        if (controller.getState() == ProgramController.State.SUSPENDED) {
          return AppFabricServiceStatus.PROGRAM_ALREADY_SUSPENDED;
        }
        controller.suspend().get();
      } else if (action.equals("resume")) {
        if (controller.getState() == ProgramController.State.ALIVE) {
          return AppFabricServiceStatus.PROGRAM_ALREADY_RUNNING;
        }
        controller.resume().get();
      }
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      return AppFabricServiceStatus.INTERNAL_ERROR;
    }
    return AppFabricServiceStatus.OK;
  }

  /**************************** Workflow/schedule APIs *****************************************************/
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-name}/{run-id}/current")
  public void workflowStatus(HttpRequest request, final HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("app-id") String appId, @PathParam("workflow-name") String workflowName,
                             @PathParam("run-id") String runId) {

    try {
      workflowClient.getWorkflowStatus(namespaceId, appId, workflowName, runId,
                                       new WorkflowClient.Callback() {
                                         @Override
                                         public void handle(WorkflowClient.Status status) {
                                           if (status.getCode() == WorkflowClient.Status.Code.NOT_FOUND) {
                                             responder.sendStatus(HttpResponseStatus.NOT_FOUND);
                                           } else if (status.getCode() == WorkflowClient.Status.Code.OK) {
                                             responder.sendByteArray(HttpResponseStatus.OK,
                                                                     status.getResult().getBytes(),
                                                                     ImmutableMultimap.of(
                                                                       HttpHeaders.Names.CONTENT_TYPE,
                                                                       "application/json; charset=utf-8"));

                                           } else {
                                             responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                                  status.getResult());
                                           }
                                         }
                                       });
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Returns next scheduled runtime of a workflow.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/nextruntime")
  public void getScheduledRunTime(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("app-id") String appId, @PathParam("workflow-id") String workflowId) {
    try {
      Id.Program id = Id.Program.from(namespaceId, appId, ProgramType.WORKFLOW, workflowId);
      List<ScheduledRuntime> runtimes = scheduler.nextScheduledRuntime(id, SchedulableProgramType.WORKFLOW);

      JsonArray array = new JsonArray();
      for (ScheduledRuntime runtime : runtimes) {
        JsonObject object = new JsonObject();
        object.addProperty("id", runtime.getScheduleId());
        object.addProperty("time", runtime.getTime());
        array.add(object);
      }
      responder.sendJson(HttpResponseStatus.OK, array);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Get Workflow schedules
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/schedules")
  public void getWorkflowSchedules(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId,
                                   @PathParam("app-id") String appId,
                                   @PathParam("workflow-id") String workflowId) {
    ApplicationSpecification appSpec = store.getApplication(Id.Application.from(namespaceId, appId));
    if (appSpec == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "App:" + appId + " not found");
      return;
    }

    List<ScheduleSpecification> specList = Lists.newArrayList();
    for (Map.Entry<String, ScheduleSpecification> entry : appSpec.getSchedules().entrySet()) {
      ScheduleSpecification spec = entry.getValue();
      if (spec.getProgram().getProgramName().equals(workflowId) &&
        spec.getProgram().getProgramType() == SchedulableProgramType.WORKFLOW) {
        specList.add(entry.getValue());
      }
    }
    responder.sendJson(HttpResponseStatus.OK, specList,
                       new TypeToken<List<ScheduleSpecification>>() { }.getType(), GSON);
  }
}
