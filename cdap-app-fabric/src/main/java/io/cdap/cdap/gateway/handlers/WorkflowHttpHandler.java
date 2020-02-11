/*
 * Copyright © 2015-2020 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers;

import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.InstanceNotFoundException;
import io.cdap.cdap.api.schedule.SchedulableProgramType;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.api.workflow.NodeValue;
import io.cdap.cdap.api.workflow.Value;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.app.mapreduce.MRJobInfoFetcher;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ProgramNotFoundException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.internal.app.runtime.schedule.SchedulerException;
import io.cdap.cdap.internal.app.runtime.schedule.TimeSchedulerService;
import io.cdap.cdap.internal.app.runtime.schedule.constraint.ConstraintCodec;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.SatisfiableTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TriggerCodec;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.dataset.DatasetCreationSpec;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.proto.DatasetSpecificationSummary;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.ScheduledRuntime;
import io.cdap.cdap.proto.WorkflowNodeStateDetail;
import io.cdap.cdap.proto.WorkflowTokenDetail;
import io.cdap.cdap.proto.WorkflowTokenNodeDetail;
import io.cdap.cdap.proto.codec.WorkflowTokenDetailCodec;
import io.cdap.cdap.proto.codec.WorkflowTokenNodeDetailCodec;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.Ids;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.WorkflowId;
import io.cdap.cdap.scheduler.ProgramScheduleService;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.DELETE;
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
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowHttpHandler.class);
  private static final Type STRING_TO_NODESTATEDETAIL_MAP_TYPE
    = new TypeToken<Map<String, WorkflowNodeStateDetail>>() { }.getType();
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(WorkflowTokenDetail.class, new WorkflowTokenDetailCodec())
    .registerTypeAdapter(WorkflowTokenNodeDetail.class, new WorkflowTokenNodeDetailCodec())
    .registerTypeAdapter(Trigger.class, new TriggerCodec())
    .registerTypeAdapter(SatisfiableTrigger.class, new TriggerCodec())
    .registerTypeAdapter(Constraint.class, new ConstraintCodec())
    .create();

  private final DatasetFramework datasetFramework;
  private final TimeSchedulerService timeScheduler;
  private final Store store;
  private final ProgramRuntimeService runtimeService;

  @Inject
  WorkflowHttpHandler(Store store, ProgramRuntimeService runtimeService,
                      TimeSchedulerService timeScheduler, MRJobInfoFetcher mrJobInfoFetcher,
                      ProgramLifecycleService lifecycleService, NamespaceQueryAdmin namespaceQueryAdmin,
                      DatasetFramework datasetFramework, DiscoveryServiceClient discoveryServiceClient,
                      ProgramScheduleService programScheduleService) {
    super(store, runtimeService, discoveryServiceClient, lifecycleService,
          mrJobInfoFetcher, namespaceQueryAdmin, programScheduleService);
    this.datasetFramework = datasetFramework;
    this.timeScheduler = timeScheduler;
    this.store = store;
    this.runtimeService = runtimeService;
  }

  @POST
  @Path("/apps/{app-id}/workflows/{workflow-name}/runs/{run-id}/suspend")
  public void suspendWorkflowRun(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId, @PathParam("app-id") String appId,
                                 @PathParam("workflow-name") String workflowName,
                                 @PathParam("run-id") String runId) throws Exception {
    ProgramController controller = getProgramController(namespaceId, appId, workflowName, runId);
    if (controller.getState() == ProgramController.State.SUSPENDED) {
      throw new ConflictException("Program run already suspended");
    }
    controller.suspend().get();
    responder.sendString(HttpResponseStatus.OK, "Program run suspended.");
  }

  @POST
  @Path("/apps/{app-id}/workflows/{workflow-name}/runs/{run-id}/resume")
  public void resumeWorkflowRun(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId, @PathParam("app-id") String appId,
                                @PathParam("workflow-name") String workflowName,
                                @PathParam("run-id") String runId) throws Exception {
    ProgramController controller = getProgramController(namespaceId, appId, workflowName, runId);
    if (controller.getState() == ProgramController.State.ALIVE) {
      throw new ConflictException("Program is already running");
    }
    controller.resume().get();
    responder.sendString(HttpResponseStatus.OK, "Program run resumed.");
  }

  /**
   * Returns the {@link ProgramController} for the given workflow program.
   */
  private ProgramController getProgramController(String namespaceId, String appId,
                                                 String workflowName, String runId) throws NotFoundException {
    ProgramId id = new ProgramId(namespaceId, appId, ProgramType.WORKFLOW, workflowName);
    ProgramRuntimeService.RuntimeInfo runtimeInfo = runtimeService.list(id).get(RunIds.fromString(runId));
    if (runtimeInfo == null) {
      throw new NotFoundException(id.run(runId));
    }
    return runtimeInfo.getController();
  }

  /**
   * Returns the previous runtime when the scheduled program ran.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/previousruntime")
  public void getPreviousScheduledRunTime(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("app-id") String appId,
                                  @PathParam("workflow-id") String workflowId)
    throws SchedulerException, NotFoundException {
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
                                  @PathParam("workflow-id") String workflowId)
    throws SchedulerException, NotFoundException {
    getScheduledRuntime(responder, namespaceId, appId, workflowId, false);
  }

  private void getScheduledRuntime(HttpResponder responder, String namespaceId, String appName, String workflowName,
                                   boolean previousRuntimeRequested) throws SchedulerException, NotFoundException {
    try {
      ApplicationId appId = new ApplicationId(namespaceId, appName);
      WorkflowId workflowId = new WorkflowId(appId, workflowName);
      Store.ensureProgramExists(workflowId, store.getApplication(appId));

      List<ScheduledRuntime> runtimes;
      if (previousRuntimeRequested) {
        runtimes = timeScheduler.previousScheduledRuntime(workflowId, SchedulableProgramType.WORKFLOW);
      } else {
        runtimes = timeScheduler.nextScheduledRuntime(workflowId, SchedulableProgramType.WORKFLOW);
      }
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(runtimes));
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
                                   @PathParam("namespace-id") String namespace,
                                   @PathParam("app-id") String application,
                                   @PathParam("workflow-id") String workflow,
                                   @QueryParam("trigger-type") String triggerType,
                                   @QueryParam("schedule-status") String scheduleStatus) throws Exception {
    doGetSchedules(responder, new NamespaceId(namespace).app(application, ApplicationId.DEFAULT_VERSION),
                   workflow, triggerType, scheduleStatus);
  }

  /**
   * Get Workflow schedules
   */
  @GET
  @Path("/apps/{app-id}/versions/{app-version}/workflows/{workflow-id}/schedules")
  public void getWorkflowSchedules(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespace,
                                   @PathParam("app-id") String application,
                                   @PathParam("app-version") String version,
                                   @PathParam("workflow-id") String workflow,
                                   @QueryParam("trigger-type") String triggerType,
                                   @QueryParam("schedule-status") String scheduleStatus) throws Exception {
    doGetSchedules(responder, new NamespaceId(namespace).app(application, version), workflow,
                   triggerType, scheduleStatus);
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
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(workflowTokenDetail, workflowTokenDetailType));
      return;
    }
    List<NodeValue> nodeValueEntries = workflowToken.getAll(key, tokenScope);
    if (nodeValueEntries.isEmpty()) {
      throw new NotFoundException(key);
    }
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(WorkflowTokenDetail.of(Collections.singletonMap(key, nodeValueEntries)),
                                   workflowTokenDetailType));
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
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(tokenAtNode, workflowTokenNodeDetailType));
      return;
    }
    if (!workflowTokenFromNode.containsKey(key)) {
      throw new NotFoundException(key);
    }
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(WorkflowTokenNodeDetail.of(Collections.singletonMap(key,
                                                                                       workflowTokenFromNode.get(key))),
                                   workflowTokenNodeDetailType));
  }

  private WorkflowToken getWorkflowToken(String namespaceId, String appName, String workflow,
                                         String runId) throws NotFoundException {
    ApplicationId appId = new ApplicationId(namespaceId, appName);
    ApplicationSpecification appSpec = store.getApplication(appId);
    if (appSpec == null) {
      throw new NotFoundException(appId);
    }
    WorkflowId workflowId = appId.workflow(workflow);
    if (!appSpec.getWorkflows().containsKey(workflow)) {
      throw new NotFoundException(workflowId);
    }
    if (store.getRun(workflowId.run(runId)) == null) {
      throw new NotFoundException(workflowId.run(runId));
    }
    return store.getWorkflowToken(workflowId, runId);
  }

  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/runs/{run-id}/nodes/state")
  public void getWorkflowNodeStates(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("app-id") String applicationId,
                                    @PathParam("workflow-id") String workflowId,
                                    @PathParam("run-id") String runId)
    throws NotFoundException {
    ApplicationId appId = Ids.namespace(namespaceId).app(applicationId);
    ApplicationSpecification appSpec = store.getApplication(appId);
    if (appSpec == null) {
      throw new ApplicationNotFoundException(appId);
    }

    ProgramId workflowProgramId = appId.workflow(workflowId);
    WorkflowSpecification workflowSpec = appSpec.getWorkflows().get(workflowProgramId.getProgram());

    if (workflowSpec == null) {
      throw new ProgramNotFoundException(workflowProgramId);
    }

    ProgramRunId workflowRunId = workflowProgramId.run(runId);
    if (store.getRun(workflowRunId) == null) {
      throw new NotFoundException(workflowRunId);
    }

    List<WorkflowNodeStateDetail> nodeStateDetails = store.getWorkflowNodeStates(workflowRunId);
    Map<String, WorkflowNodeStateDetail> nodeStates = new HashMap<>();
    for (WorkflowNodeStateDetail nodeStateDetail : nodeStateDetails) {
      nodeStates.put(nodeStateDetail.getNodeId(), nodeStateDetail);
    }

    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(nodeStates, STRING_TO_NODESTATEDETAIL_MAP_TYPE));
  }

  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/runs/{run-id}/localdatasets")
  public void getWorkflowLocalDatasets(HttpRequest request, HttpResponder responder,
                                       @PathParam("namespace-id") String namespaceId,
                                       @PathParam("app-id") String applicationId,
                                       @PathParam("workflow-id") String workflowId,
                                       @PathParam("run-id") String runId)
    throws NotFoundException, DatasetManagementException {
    WorkflowSpecification workflowSpec = getWorkflowSpecForValidRun(namespaceId, applicationId, workflowId, runId);
    Map<String, DatasetSpecificationSummary> localDatasetSummaries = new HashMap<>();
    for (Map.Entry<String, DatasetCreationSpec> localDatasetEntry : workflowSpec.getLocalDatasetSpecs().entrySet()) {
      String mappedDatasetName = localDatasetEntry.getKey() + "." + runId;
      String datasetType = localDatasetEntry.getValue().getTypeName();
      Map<String, String> datasetProperties = localDatasetEntry.getValue().getProperties().getProperties();
      if (datasetFramework.hasInstance(new DatasetId(namespaceId, mappedDatasetName))) {
        localDatasetSummaries.put(localDatasetEntry.getKey(),
                                  new DatasetSpecificationSummary(mappedDatasetName, datasetType, datasetProperties));
      }
    }

    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(localDatasetSummaries));
  }

  @DELETE
  @Path("/apps/{app-id}/workflows/{workflow-id}/runs/{run-id}/localdatasets")
  public void deleteWorkflowLocalDatasets(HttpRequest request, HttpResponder responder,
                                       @PathParam("namespace-id") String namespaceId,
                                       @PathParam("app-id") String applicationId,
                                       @PathParam("workflow-id") String workflowId,
                                       @PathParam("run-id") String runId) throws NotFoundException {
    WorkflowSpecification workflowSpec = getWorkflowSpecForValidRun(namespaceId, applicationId, workflowId, runId);
    Set<String> errorOnDelete = new HashSet<>();
    for (Map.Entry<String, DatasetCreationSpec> localDatasetEntry : workflowSpec.getLocalDatasetSpecs().entrySet()) {
      String mappedDatasetName = localDatasetEntry.getKey() + "." + runId;
      // try best to delete the local datasets.
      try {
        datasetFramework.deleteInstance(new DatasetId(namespaceId, mappedDatasetName));
      } catch (InstanceNotFoundException e) {
        // Dataset instance is already deleted. so its no-op.
      } catch (Throwable t) {
        errorOnDelete.add(mappedDatasetName);
        LOG.error("Failed to delete the Workflow local dataset {}. Reason - {}", mappedDatasetName, t.getMessage());
      }
    }

    if (errorOnDelete.isEmpty()) {
      responder.sendStatus(HttpResponseStatus.OK);
      return;
    }

    String errorMessage = "Failed to delete Workflow local datasets - " + Joiner.on(",").join(errorOnDelete);
    throw new RuntimeException(errorMessage);
  }

  /**
   * Get the {@link WorkflowSpecification} if valid application id, workflow id, and runid are provided.
   * @param namespaceId the namespace id
   * @param applicationId the application id
   * @param workflowId the workflow id
   * @param runId the runid of the workflow
   * @return the specifications for the Workflow
   * @throws NotFoundException is thrown when the application, workflow, or runid is not found
   */
  private WorkflowSpecification getWorkflowSpecForValidRun(String namespaceId, String applicationId,
                                                           String workflowId, String runId) throws NotFoundException {
    ApplicationId appId = new ApplicationId(namespaceId, applicationId);
    ApplicationSpecification appSpec = store.getApplication(appId);
    if (appSpec == null) {
      throw new ApplicationNotFoundException(appId);
    }

    WorkflowSpecification workflowSpec = appSpec.getWorkflows().get(workflowId);
    ProgramId programId = new ProgramId(namespaceId, applicationId, ProgramType.WORKFLOW, workflowId);
    if (workflowSpec == null) {
      throw new ProgramNotFoundException(programId);
    }

    if (store.getRun(programId.run(runId)) == null) {
      throw new NotFoundException(new ProgramRunId(programId.getNamespace(), programId.getApplication(),
                                                   programId.getType(), programId.getProgram(), runId));
    }
    return workflowSpec;
  }
}
