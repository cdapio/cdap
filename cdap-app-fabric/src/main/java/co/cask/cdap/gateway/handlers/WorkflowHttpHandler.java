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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.InstanceNotFoundException;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.workflow.NodeValue;
import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.mapreduce.MRJobInfoFetcher;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.ConflictException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerException;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.internal.dataset.DatasetCreationSpec;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ScheduledRuntime;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.WorkflowTokenDetail;
import co.cask.cdap.proto.WorkflowTokenNodeDetail;
import co.cask.cdap.proto.codec.ScheduleSpecificationCodec;
import co.cask.cdap.proto.codec.WorkflowTokenDetailCodec;
import co.cask.cdap.proto.codec.WorkflowTokenNodeDetailCodec;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.http.HttpResponder;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
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
    .registerTypeAdapter(ScheduleSpecification.class, new ScheduleSpecificationCodec())
    .registerTypeAdapter(WorkflowTokenDetail.class, new WorkflowTokenDetailCodec())
    .registerTypeAdapter(WorkflowTokenNodeDetail.class, new WorkflowTokenNodeDetailCodec())
    .create();

  private final WorkflowClient workflowClient;
  private final DatasetFramework datasetFramework;

  @Inject
  WorkflowHttpHandler(Store store, WorkflowClient workflowClient, ProgramRuntimeService runtimeService,
                      QueueAdmin queueAdmin, Scheduler scheduler, PreferencesStore preferencesStore,
                      MRJobInfoFetcher mrJobInfoFetcher, ProgramLifecycleService lifecycleService,
                      MetricStore metricStore, DatasetFramework datasetFramework,
                      DiscoveryServiceClient discoveryServiceClient) {
    super(store, runtimeService, discoveryServiceClient, lifecycleService, queueAdmin, scheduler, preferencesStore,
          mrJobInfoFetcher, metricStore);
    this.workflowClient = workflowClient;
    this.datasetFramework = datasetFramework;
  }

  @POST
  @Path("/apps/{app-id}/workflows/{workflow-name}/runs/{run-id}/suspend")
  public void suspendWorkflowRun(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId, @PathParam("app-id") String appId,
                                 @PathParam("workflow-name") String workflowName,
                                 @PathParam("run-id") String runId) throws Exception {
    Id.Program id = Id.Program.from(namespaceId, appId, ProgramType.WORKFLOW, workflowName);
    ProgramRuntimeService.RuntimeInfo runtimeInfo = runtimeService.list(id).get(RunIds.fromString(runId));
    if (runtimeInfo == null) {
      throw new NotFoundException(new Id.Run(id, runId));
    }
    ProgramController controller = runtimeInfo.getController();
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

    Id.Program id = Id.Program.from(namespaceId, appId, ProgramType.WORKFLOW, workflowName);
    ProgramRuntimeService.RuntimeInfo runtimeInfo = runtimeService.list(id).get(RunIds.fromString(runId));
    if (runtimeInfo == null) {
      throw new NotFoundException(new Id.Run(id, runId));
    }
    ProgramController controller = runtimeInfo.getController();
    if (controller.getState() == ProgramController.State.ALIVE) {
      throw new ConflictException("Program is already running");
    }
    controller.resume().get();
    responder.sendString(HttpResponseStatus.OK, "Program run resumed.");
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
      Id.Application appId = Id.Application.from(namespaceId, appName);
      Id.Program workflowId = Id.Program.from(appId, ProgramType.WORKFLOW, workflowName);
      ApplicationSpecification appSpec = store.getApplication(appId);
      if (appSpec == null) {
        throw new ApplicationNotFoundException(appId);
      }
      if (appSpec.getWorkflows().get(workflowName) == null) {
        throw new ProgramNotFoundException(workflowId);
      }
      List<ScheduledRuntime> runtimes;
      if (previousRuntimeRequested) {
        runtimes = scheduler.previousScheduledRuntime(workflowId, SchedulableProgramType.WORKFLOW);
      } else {
        runtimes = scheduler.nextScheduledRuntime(workflowId, SchedulableProgramType.WORKFLOW);
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

  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/runs/{run-id}/nodes/state")
  public void getWorkflowNodeStates(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("app-id") String applicationId,
                                    @PathParam("workflow-id") String workflowId,
                                    @PathParam("run-id") String runId)
    throws NotFoundException {
    ApplicationId appId = Ids.namespace(namespaceId).app(applicationId);
    ApplicationSpecification appSpec = store.getApplication(appId.toId());
    if (appSpec == null) {
      throw new ApplicationNotFoundException(appId.toId());
    }

    ProgramId workflowProgramId = appId.workflow(workflowId);
    WorkflowSpecification workflowSpec = appSpec.getWorkflows().get(workflowProgramId.getProgram());

    if (workflowSpec == null) {
      throw new ProgramNotFoundException(workflowProgramId.toId());
    }

    ProgramRunId workflowRunId = workflowProgramId.run(runId);
    if (store.getRun(workflowProgramId.toId(), runId) == null) {
      throw new NotFoundException(workflowRunId);
    }

    List<WorkflowNodeStateDetail> nodeStateDetails = store.getWorkflowNodeStates(workflowRunId);
    Map<String, WorkflowNodeStateDetail> nodeStates = new HashMap<>();
    for (WorkflowNodeStateDetail nodeStateDetail : nodeStateDetails) {
      nodeStates.put(nodeStateDetail.getNodeId(), nodeStateDetail);
    }

    responder.sendJson(HttpResponseStatus.OK, nodeStates, STRING_TO_NODESTATEDETAIL_MAP_TYPE);
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
      if (datasetFramework.hasInstance(Id.DatasetInstance.from(namespaceId, mappedDatasetName))) {
        localDatasetSummaries.put(localDatasetEntry.getKey(),
                                  new DatasetSpecificationSummary(mappedDatasetName, datasetType, datasetProperties));
      }
    }

    responder.sendJson(HttpResponseStatus.OK, localDatasetSummaries);
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
        datasetFramework.deleteInstance(Id.DatasetInstance.from(namespaceId, mappedDatasetName));
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
    ApplicationSpecification appSpec = store.getApplication(appId.toId());
    if (appSpec == null) {
      throw new ApplicationNotFoundException(appId.toId());
    }

    WorkflowSpecification workflowSpec = appSpec.getWorkflows().get(workflowId);
    ProgramId programId = new ProgramId(namespaceId, applicationId, ProgramType.WORKFLOW, workflowId);
    if (workflowSpec == null) {
      throw new ProgramNotFoundException(programId.toId());
    }

    if (store.getRun(programId.toId(), runId) == null) {
      throw new NotFoundException(new ProgramRunId(programId.getNamespace(), programId.getApplication(),
                                                   programId.getType(), programId.getProgram(), runId));
    }
    return workflowSpec;
  }
}
