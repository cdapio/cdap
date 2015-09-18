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

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.workflow.NodeValue;
import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowToken;
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
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerException;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.internal.app.services.PropertiesResolver;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ScheduledRuntime;
import co.cask.cdap.proto.WorkflowTokenDetail;
import co.cask.cdap.proto.WorkflowTokenNodeDetail;
import co.cask.cdap.proto.codec.ScheduleSpecificationCodec;
import co.cask.cdap.proto.codec.WorkflowTokenDetailCodec;
import co.cask.cdap.proto.codec.WorkflowTokenNodeDetailCodec;
import co.cask.http.HttpResponder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Workflow HTTP Handler.
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class WorkflowHttpHandler extends ProgramLifecycleHttpHandler {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ScheduleSpecification.class, new ScheduleSpecificationCodec())
    .registerTypeAdapter(WorkflowTokenDetail.class, new WorkflowTokenDetailCodec())
    .registerTypeAdapter(WorkflowTokenNodeDetail.class, new WorkflowTokenNodeDetailCodec())
    .create();

  private final WorkflowClient workflowClient;

  @Inject
  public WorkflowHttpHandler(Store store, WorkflowClient workflowClient,
                             CConfiguration configuration, ProgramRuntimeService runtimeService,
                             QueueAdmin queueAdmin, Scheduler scheduler, PreferencesStore preferencesStore,
                             NamespacedLocationFactory namespacedLocationFactory, MRJobInfoFetcher mrJobInfoFetcher,
                             ProgramLifecycleService lifecycleService, PropertiesResolver resolver,
                             MetricStore metricStore) {
    super(store, configuration, runtimeService, lifecycleService, queueAdmin, scheduler,
          preferencesStore, namespacedLocationFactory, mrJobInfoFetcher, resolver, metricStore);
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

  // TODO: CDAP-2481. Deprecated API. Remove in 3.2.
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-name}/{run-id}/current")
  public void getWorkflowStatusOld(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("app-id") String appId, @PathParam("workflow-name") String workflowName,
                                  @PathParam("run-id") String runId) throws IOException {
    getWorkflowStatus(request, responder, namespaceId, appId, workflowName, runId);
  }

  @GET
  @Path("/apps/{app-id}/workflows/{workflow-name}/runs/{run-id}/current")
  public void getWorkflowStatus(HttpRequest request, final HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("app-id") String appId, @PathParam("workflow-name") String workflowName,
                                @PathParam("run-id") String runId) throws IOException {
    try {
      workflowClient.getWorkflowStatus(namespaceId, appId, workflowName, runId,
                                       new WorkflowClient.Callback() {
                                         @Override
                                         public void handle(WorkflowClient.Status status) {
                                           if (status.getCode() == WorkflowClient.Status.Code.NOT_FOUND) {
                                             responder.sendStatus(HttpResponseStatus.NOT_FOUND);
                                           } else if (status.getCode() == WorkflowClient.Status.Code.OK) {
                                             // This uses responder.sendByteArray because status.getResult returns a
                                             // json string, and responder.sendJson would need deserialization and
                                             // serialization.
                                             responder.sendByteArray(HttpResponseStatus.OK,
                                                                     Bytes.toBytes(status.getResult()),
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
    }
  }

  /**
   * Returns the previous runtime when the scheduled program ran.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/previousruntime")
  public void getPreviousScheduledRunTime(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("app-id") String appId,
                                  @PathParam("workflow-id") String workflowId) throws SchedulerException {
    getScheduledRuntime(responder, namespaceId, appId, workflowId, true);
  }

  /**
   * Returns next scheduled runtime of a workflow.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/nextruntime")
  public void getNextScheduledRunTime(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("app-id") String appId,
                                  @PathParam("workflow-id") String workflowId) throws SchedulerException {
    getScheduledRuntime(responder, namespaceId, appId, workflowId, false);
  }

  private void getScheduledRuntime(HttpResponder responder, String namespaceId, String appId, String workflowId,
                                   boolean previousRuntimeRequested) throws SchedulerException {
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

  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/runs/{run-id}/token")
  public void getWorkflowToken(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("app-id") String appId,
                               @PathParam("workflow-id") String workflowId,
                               @PathParam("run-id") String runId,
                               @QueryParam("scope") @DefaultValue("user") String scope,
                               @QueryParam("key") @DefaultValue("") String key) throws NotFoundException {
    WorkflowToken workflowToken = getWorkflowToken(namespaceId, appId, workflowId, runId);
    WorkflowToken.Scope tokenScope = WorkflowToken.Scope.valueOf(scope.toUpperCase());
    WorkflowTokenDetail workflowTokenDetail = WorkflowTokenDetail.of(workflowToken.getAll(tokenScope));
    Type workflowTokenDetailType = new TypeToken<WorkflowTokenDetail>() { }.getType();
    if (key.isEmpty()) {
      responder.sendJson(HttpResponseStatus.OK, workflowTokenDetail, workflowTokenDetailType, GSON);
      return;
    }
    List<NodeValue> nodeValueEntries = workflowToken.getAll(key, tokenScope);
    if (nodeValueEntries.isEmpty()) {
      throw new NotFoundException(key);
    }
    responder.sendJson(HttpResponseStatus.OK, WorkflowTokenDetail.of(ImmutableMap.of(key, nodeValueEntries)),
                       workflowTokenDetailType, GSON);
  }

  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/runs/{run-id}/nodes/{node-id}/token")
  public void getWorkflowToken(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("app-id") String appId,
                               @PathParam("workflow-id") String workflowId,
                               @PathParam("run-id") String runId,
                               @PathParam("node-id") String nodeId,
                               @QueryParam("scope") @DefaultValue("user") String scope,
                               @QueryParam("key") @DefaultValue("") String key) throws NotFoundException {
    WorkflowToken workflowToken = getWorkflowToken(namespaceId, appId, workflowId, runId);
    WorkflowToken.Scope tokenScope = WorkflowToken.Scope.valueOf(scope.toUpperCase());
    Map<String, Value> workflowTokenFromNode = workflowToken.getAllFromNode(nodeId, tokenScope);
    WorkflowTokenNodeDetail tokenAtNode = WorkflowTokenNodeDetail.of(workflowTokenFromNode);
    Type workflowTokenNodeDetailType = new TypeToken<WorkflowTokenNodeDetail>() { }.getType();
    if (key.isEmpty()) {
      responder.sendJson(HttpResponseStatus.OK, tokenAtNode, workflowTokenNodeDetailType, GSON);
      return;
    }
    if (!workflowTokenFromNode.containsKey(key)) {
      throw new NotFoundException(key);
    }
    responder.sendJson(HttpResponseStatus.OK,
                       WorkflowTokenNodeDetail.of(ImmutableMap.of(key, workflowTokenFromNode.get(key))),
                       workflowTokenNodeDetailType, GSON);
  }

  private WorkflowToken getWorkflowToken(String namespaceId, String appName, String workflow,
                                         String runId) throws NotFoundException {
    Id.Application appId = Id.Application.from(namespaceId, appName);
    ApplicationSpecification appSpec = store.getApplication(appId);
    if (appSpec == null) {
      throw new NotFoundException(appId);
    }
    Id.Workflow workflowId = Id.Workflow.from(appId, workflow);
    if (!appSpec.getWorkflows().containsKey(workflow)) {
      throw new NotFoundException(workflowId);
    }
    if (store.getRun(workflowId, runId) == null) {
      throw new NotFoundException(new Id.Run(workflowId, runId));
    }
    return store.getWorkflowToken(workflowId, runId);
  }
}
