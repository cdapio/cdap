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

import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.mapreduce.MRJobInfoFetcher;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.internal.app.runtime.adapter.AdapterService;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.internal.app.services.PropertiesResolver;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ScheduledRuntime;
import co.cask.http.HttpResponder;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
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

  private final WorkflowClient workflowClient;

  @Inject
  public WorkflowHttpHandler(Store store, WorkflowClient workflowClient,
                             CConfiguration configuration, ProgramRuntimeService runtimeService,
                             QueueAdmin queueAdmin, Scheduler scheduler, PreferencesStore preferencesStore,
                             NamespacedLocationFactory namespacedLocationFactory, MRJobInfoFetcher mrJobInfoFetcher,
                             ProgramLifecycleService lifecycleService, PropertiesResolver resolver,
                             AdapterService adapterService, MetricStore metricStore) {
    super(store, configuration, runtimeService, lifecycleService, queueAdmin, scheduler,
          preferencesStore, namespacedLocationFactory, mrJobInfoFetcher, resolver, adapterService, metricStore);
    this.workflowClient = workflowClient;
  }

  @POST
  @Path("/apps/{app-id}/workflows/{workflow-name}/runs/{run-id}/suspend")
  public void suspendWorkflowRun(HttpRequest request, final HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId, @PathParam("app-id") String appId,
                                 @PathParam("workflow-name") String workflowName, @PathParam("run-id") String runId)
    throws NotFoundException, ExecutionException, InterruptedException {

    Id.Program id = Id.Program.from(namespaceId, appId, ProgramType.WORKFLOW, workflowName);
    ProgramRuntimeService.RuntimeInfo runtimeInfo = runtimeService.list(id).get(RunIds.fromString(runId));
    if (runtimeInfo == null) {
      throw new NotFoundException(new Id.Run(id, runId));
    }
    ProgramController controller = runtimeInfo.getController();
    if (controller.getState() == ProgramController.State.SUSPENDED) {
      responder.sendString(AppFabricServiceStatus.PROGRAM_ALREADY_SUSPENDED.getCode(),
                           AppFabricServiceStatus.PROGRAM_ALREADY_SUSPENDED.getMessage());
      return;
    }
    controller.suspend().get();
    responder.sendString(HttpResponseStatus.OK, "Program run suspended.");
  }

  @POST
  @Path("/apps/{app-id}/workflows/{workflow-name}/runs/{run-id}/resume")
  public void resumeWorkflowRun(HttpRequest request, final HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId, @PathParam("app-id") String appId,
                                @PathParam("workflow-name") String workflowName, @PathParam("run-id") String runId)
          throws NotFoundException, ExecutionException, InterruptedException {

    Id.Program id = Id.Program.from(namespaceId, appId, ProgramType.WORKFLOW, workflowName);
    ProgramRuntimeService.RuntimeInfo runtimeInfo = runtimeService.list(id).get(RunIds.fromString(runId));
    if (runtimeInfo == null) {
      throw new NotFoundException(new Id.Run(id, runId));
    }
    ProgramController controller = runtimeInfo.getController();
    if (controller.getState() == ProgramController.State.ALIVE) {
      responder.sendString(AppFabricServiceStatus.PROGRAM_ALREADY_RUNNING.getCode(),
                           AppFabricServiceStatus.PROGRAM_ALREADY_RUNNING.getMessage());
      return;
    }
    controller.resume().get();
    responder.sendString(HttpResponseStatus.OK, "Program run resumed.");
  }

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
   * Returns the previous runtime when the scheduled program ran.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/previousruntime")
  public void getPreviousScheduledRunTime(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("app-id") String appId, @PathParam("workflow-id") String workflowId) {
    getScheduledRuntime(responder, namespaceId, appId, workflowId, true);
  }

  /**
   * Returns next scheduled runtime of a workflow.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/nextruntime")
  public void getNextScheduledRunTime(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("app-id") String appId, @PathParam("workflow-id") String workflowId) {
    getScheduledRuntime(responder, namespaceId, appId, workflowId, false);
  }

  private void getScheduledRuntime(HttpResponder responder, String namespaceId, String appId, String workflowId,
                                   boolean previousRuntimeRequested) {
    try {
      Id.Program id = Id.Program.from(namespaceId, appId, ProgramType.WORKFLOW, workflowId);
      List<ScheduledRuntime> runtimes;
      if (previousRuntimeRequested) {
        runtimes = scheduler.previousScheduledRuntime(id, SchedulableProgramType.WORKFLOW);
      } else {
        runtimes = scheduler.nextScheduledRuntime(id, SchedulableProgramType.WORKFLOW);
      }
      responder.sendJson(HttpResponseStatus.OK, runtimes);
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
