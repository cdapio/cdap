/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.internal;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.schedule.Trigger;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.gateway.handlers.AppLifecycleHttpHandler;
import co.cask.cdap.gateway.handlers.NamespaceHttpHandler;
import co.cask.cdap.gateway.handlers.ProgramLifecycleHttpHandler;
import co.cask.cdap.gateway.handlers.WorkflowHttpHandler;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.app.BufferFileInputStream;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.proto.ApplicationDetail;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.Instances;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.PluginInstanceDetail;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ProtoConstraintCodec;
import co.cask.cdap.proto.ProtoTriggerCodec;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.ScheduleDetail;
import co.cask.cdap.proto.ServiceInstances;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.WorkflowTokenDetail;
import co.cask.cdap.proto.WorkflowTokenNodeDetail;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.codec.ScheduleSpecificationCodec;
import co.cask.cdap.proto.codec.WorkflowTokenDetailCodec;
import co.cask.cdap.proto.codec.WorkflowTokenNodeDetailCodec;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.http.BodyConsumer;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Client tool for AppFabricHttpHandler.
 */
public class AppFabricClient {
  private static final Logger LOG = LoggerFactory.getLogger(AppFabricClient.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ScheduleSpecification.class, new ScheduleSpecificationCodec())
    .registerTypeAdapter(WorkflowTokenDetail.class, new WorkflowTokenDetailCodec())
    .registerTypeAdapter(WorkflowTokenNodeDetail.class, new WorkflowTokenNodeDetailCodec())
    .registerTypeAdapter(Trigger.class, new ProtoTriggerCodec())
    .registerTypeAdapter(Constraint.class, new ProtoConstraintCodec())
    .create();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type RUN_RECORDS_TYPE = new TypeToken<List<RunRecord>>() { }.getType();
  private static final Type SCHEDULE_DETAILS_TYPE = new TypeToken<List<ScheduleDetail>>() { }.getType();
  private static final Type SCHEDULE_SPECS_TYPE = new TypeToken<List<ScheduleSpecification>>() { }.getType();

  private final LocationFactory locationFactory;
  private final AppLifecycleHttpHandler appLifecycleHttpHandler;
  private final ProgramLifecycleHttpHandler programLifecycleHttpHandler;
  private final WorkflowHttpHandler workflowHttpHandler;
  private final NamespaceHttpHandler namespaceHttpHandler;
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  @Inject
  public AppFabricClient(LocationFactory locationFactory,
                         AppLifecycleHttpHandler appLifecycleHttpHandler,
                         ProgramLifecycleHttpHandler programLifecycleHttpHandler,
                         NamespaceHttpHandler namespaceHttpHandler,
                         NamespaceQueryAdmin namespaceQueryAdmin,
                         WorkflowHttpHandler workflowHttpHandler) {
    this.locationFactory = locationFactory;
    this.appLifecycleHttpHandler = appLifecycleHttpHandler;
    this.programLifecycleHttpHandler = programLifecycleHttpHandler;
    this.namespaceHttpHandler = namespaceHttpHandler;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.workflowHttpHandler = workflowHttpHandler;
  }

  private String getNamespacePath(String namespaceId) {
    return String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, namespaceId);
  }

  public void reset() throws Exception {
    MockResponder responder;
    HttpRequest request;

    // delete all namespaces
    for (NamespaceMeta namespaceMeta : namespaceQueryAdmin.list()) {
      Id.Namespace namespace = Id.Namespace.from(namespaceMeta.getName());

      responder = new MockResponder();
      request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.DELETE, String.format(
        "%s/unrecoverable/namespaces/%s/datasets", Constants.Gateway.API_VERSION_3, namespace.getId()));
      namespaceHttpHandler.deleteDatasets(request, responder, namespaceMeta.getName());
      verifyResponse(HttpResponseStatus.OK, responder.getStatus(),
                     String.format("could not delete datasets in namespace '%s'", namespace.getId()));

      responder = new MockResponder();
      request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.DELETE,
                                       String.format("/v3/unrecoverable/namespaces/%s", namespace.getId()));
      namespaceHttpHandler.delete(request, responder, namespaceMeta.getName());
      verifyResponse(HttpResponseStatus.OK, responder.getStatus(),
                     String.format("could not delete namespace '%s'", namespace.getId()));
    }
  }

  public void startProgram(String namespaceId, String appId,
                           String flowId, ProgramType type, Map<String, String> args) throws Exception {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/%s/%s/start",
                               getNamespacePath(namespaceId), appId, type.getCategoryName(), flowId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    String argString = GSON.toJson(args);
    if (argString != null) {
      request.setContent(ChannelBuffers.wrappedBuffer(argString.getBytes(Charsets.UTF_8)));
    }
    programLifecycleHttpHandler.performAction(request, responder, namespaceId, appId,
                                              type.getCategoryName(), flowId, "start");
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Start " + type + " failed");
  }

  public void startProgram(String namespaceId, String appId, String appVersion, String programId,
                           ProgramType type, Map<String, String> args) throws Exception {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/versions/%s/%s/%s/start", getNamespacePath(namespaceId), appId, appVersion,
                               type.getCategoryName(), programId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    String argString = GSON.toJson(args);
    if (argString != null) {
      request.setContent(ChannelBuffers.wrappedBuffer(argString.getBytes(Charsets.UTF_8)));
    }
    programLifecycleHttpHandler.performAction(request, responder, namespaceId, appId, appVersion,
                                              type.getCategoryName(), programId, "start");
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Start " + type + " failed");
  }


  public void stopProgram(String namespaceId, String appId, String programId, ProgramType type) throws Exception {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/%s/%s/stop",
                               getNamespacePath(namespaceId), appId, type.getCategoryName(), programId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    programLifecycleHttpHandler.performAction(request, responder, namespaceId, appId,
                                              type.getCategoryName(), programId, "stop");
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Stop " + type + " failed");
  }

  public void stopProgram(String namespaceId, String appId, String appVersion, String programId,
                          ProgramType type) throws Exception {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/versions/%s/%s/%s/stop",
                               getNamespacePath(namespaceId), appId, appVersion, type.getCategoryName(), programId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    programLifecycleHttpHandler.performAction(request, responder, namespaceId, appId, appVersion,
                                              type.getCategoryName(), programId, "stop");
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Stop " + type + " failed");
  }

  public String getStatus(String namespaceId, String appId, String programId, ProgramType type)
    throws Exception {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/%s/%s/status", getNamespacePath(namespaceId), appId, type, programId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    programLifecycleHttpHandler.getStatus(request, responder, namespaceId, appId, type.getCategoryName(), programId);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Get status " + type + " failed");
    Map<String, String> json = responder.decodeResponseContent(MAP_TYPE);
    return json.get("status");
  }

  public String getStatus(String namespaceId, String appId, String appVersion, String programId,
                          ProgramType type) throws Exception {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/versions/%s/%s/%s/status", getNamespacePath(namespaceId),
                               appId, appVersion, type, programId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    programLifecycleHttpHandler.getStatus(request, responder, namespaceId, appId, appVersion, type.getCategoryName(),
                                          programId);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Get status " + type + " failed");
    Map<String, String> json = responder.decodeResponseContent(MAP_TYPE);
    return json.get("status");
  }

  public void setWorkerInstances(String namespaceId, String appId, String workerId, int instances) throws Exception {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/worker/%s/instances", getNamespacePath(namespaceId), appId, workerId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, uri);
    JsonObject json = new JsonObject();
    json.addProperty("instances", instances);
    request.setContent(ChannelBuffers.wrappedBuffer(json.toString().getBytes()));
    programLifecycleHttpHandler.setWorkerInstances(request, responder, namespaceId, appId, workerId);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Set worker instances failed");
  }

  public Instances getWorkerInstances(String namespaceId, String appId, String workerId) throws Exception {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/worker/%s/instances", getNamespacePath(namespaceId), appId, workerId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    programLifecycleHttpHandler.getWorkerInstances(request, responder, namespaceId, appId, workerId);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Get worker instances failed");
    return responder.decodeResponseContent(Instances.class);
  }

  public void setServiceInstances(String namespaceId, String applicationId, String serviceName,
                                  int instances) throws Exception {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/services/%s/instances",
                               getNamespacePath(namespaceId), applicationId, serviceName);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, uri);
    JsonObject json = new JsonObject();
    json.addProperty("instances", instances);
    request.setContent(ChannelBuffers.wrappedBuffer(json.toString().getBytes()));
    programLifecycleHttpHandler.setServiceInstances(request, responder, namespaceId, applicationId, serviceName);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Set service instances failed");
  }

  public ServiceInstances getServiceInstances(String namespaceId, String applicationId, String serviceName)
    throws Exception {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/services/%s/instances",
                               getNamespacePath(namespaceId), applicationId, serviceName);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    programLifecycleHttpHandler.getServiceInstances(request, responder, namespaceId, applicationId, serviceName);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Get service instances failed");
    return responder.decodeResponseContent(ServiceInstances.class);
  }

  public void setFlowletInstances(String namespaceId, String applicationId, String flowId, String flowletName,
                                  int instances) throws Exception {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/flows/%s/flowlets/%s/instances/%s",
                               getNamespacePath(namespaceId), applicationId, flowId, flowletName, instances);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, uri);
    JsonObject json = new JsonObject();
    json.addProperty("instances", instances);
    request.setContent(ChannelBuffers.wrappedBuffer(json.toString().getBytes()));
    programLifecycleHttpHandler.setFlowletInstances(request, responder, namespaceId,
                                                    applicationId, flowId, flowletName);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Set flowlet instances failed");
  }

  public Instances getFlowletInstances(String namespaceId, String applicationId, String flowName,
                                       String flowletName) throws Exception {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/flows/%s/flowlets/%s/instances",
                               getNamespacePath(namespaceId), applicationId, flowName, flowletName);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    programLifecycleHttpHandler.getFlowletInstances(request, responder, namespaceId, applicationId, flowName,
                                                    flowletName);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Get flowlet instances failed");
    return responder.decodeResponseContent(Instances.class);
  }

  public List<ScheduleDetail> getProgramSchedules(String namespace, String app, String workflow)
    throws NotFoundException {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/workflows/%s/schedules",
                               getNamespacePath(namespace), app, workflow);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    try {
      workflowHttpHandler.getWorkflowSchedules(request, responder, namespace, app, workflow, null, null);
    } catch (BadRequestException e) {
      // cannot happen
      throw Throwables.propagate(e);
    }

    List<ScheduleDetail> schedules = responder.decodeResponseContent(SCHEDULE_DETAILS_TYPE, GSON);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Getting workflow schedules failed");
    return schedules;
  }

  /**
   * Return the specifications of all schedules of a workflow.
   *
   * @deprecated since release 4.2. Use {@link #getProgramSchedules(String, String, String)} instead.
   */
  @Deprecated
  public List<ScheduleSpecification> getSchedules(String namespace, String app, String workflow)
    throws NotFoundException {
    return ScheduleDetail.toScheduleSpecs(getProgramSchedules(namespace, app, workflow));
  }

  public WorkflowTokenDetail getWorkflowToken(String namespaceId, String appId, String wflowId, String runId,
                                              @Nullable WorkflowToken.Scope scope,
                                              @Nullable String key) throws NotFoundException {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/workflows/%s/runs/%s/token",
                               getNamespacePath(namespaceId), appId, wflowId, runId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    scope = scope == null ? WorkflowToken.Scope.USER : scope;
    key = key == null ? "" : key;
    workflowHttpHandler.getWorkflowToken(request, responder, namespaceId, appId, wflowId, runId, scope.name(), key);

    Type workflowTokenDetailType = new TypeToken<WorkflowTokenDetail>() { }.getType();
    WorkflowTokenDetail workflowTokenDetail = responder.decodeResponseContent(workflowTokenDetailType, GSON);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Getting workflow token failed");
    return workflowTokenDetail;
  }

  public WorkflowTokenNodeDetail getWorkflowToken(String namespaceId, String appId, String wflowId, String runId,
                                                  String nodeName, @Nullable WorkflowToken.Scope scope,
                                                  @Nullable String key) throws NotFoundException {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/workflows/%s/runs/%s/nodes/%s/token",
                               getNamespacePath(namespaceId), appId, wflowId, runId, nodeName);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    scope = scope == null ? WorkflowToken.Scope.USER : scope;
    key = key == null ? "" : key;
    workflowHttpHandler.getWorkflowToken(request, responder, namespaceId, appId, wflowId, runId, nodeName,
                                         scope.name(), key);

    Type workflowTokenNodeDetailType = new TypeToken<WorkflowTokenNodeDetail>() { }.getType();
    WorkflowTokenNodeDetail workflowTokenDetail = responder.decodeResponseContent(workflowTokenNodeDetailType, GSON);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Getting workflow token at node failed");
    return workflowTokenDetail;
  }

  public Map<String, WorkflowNodeStateDetail> getWorkflowNodeStates(ProgramRunId workflowRunId)
    throws NotFoundException {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/workflows/%s/runs/%s/nodes/state",
                               getNamespacePath(workflowRunId.getNamespace()), workflowRunId.getApplication(),
                               workflowRunId.getProgram(), workflowRunId.getRun());
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    workflowHttpHandler.getWorkflowNodeStates(request, responder, workflowRunId.getNamespace(),
                                              workflowRunId.getApplication(), workflowRunId.getProgram(),
                                              workflowRunId.getRun());

    Type nodeStatesType = new TypeToken<Map<String, WorkflowNodeStateDetail>>() { }.getType();
    Map<String, WorkflowNodeStateDetail> nodeStates = responder.decodeResponseContent(nodeStatesType, GSON);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Getting workflow node states failed.");
    return nodeStates;
  }

  public List<RunRecord> getHistory(Id.Program programId, ProgramRunStatus status) throws Exception {
    String namespace = programId.getNamespaceId();
    String application = programId.getApplicationId();
    String programName = programId.getId();
    String categoryName = programId.getType().getCategoryName();

    return doGetHistory(namespace, application, ApplicationId.DEFAULT_VERSION, programName, categoryName, status);
  }

  public List<RunRecord> getHistory(ProgramId programId, ProgramRunStatus status) throws Exception {
    String namespace = programId.getNamespace();
    String application = programId.getApplication();
    String applicationVersion = programId.getVersion();
    String programName = programId.getProgram();
    String categoryName = programId.getType().getCategoryName();

    return doGetHistory(namespace, application, applicationVersion, programName, categoryName, status);
  }

  private List<RunRecord> doGetHistory(String namespace, String application, String applicationVersion,
                                       String programName, String categoryName, ProgramRunStatus status)
    throws Exception {

    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/versions/%s/%s/runs?status=" + status.name(),
                               getNamespacePath(namespace), application, applicationVersion, categoryName, programName);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    programLifecycleHttpHandler.programHistory(request, responder, namespace, application, applicationVersion,
                                               categoryName, programName, status.name(), null, null, 100);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Getting workflow history failed");

    return responder.decodeResponseContent(RUN_RECORDS_TYPE);
  }

  public void suspend(String namespaceId, String appId, String scheduleName) throws Exception {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/schedules/%s/suspend", getNamespacePath(namespaceId), appId, scheduleName);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    programLifecycleHttpHandler.performAction(request, responder, namespaceId, appId,
                                              "schedules", scheduleName, "suspend");
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Suspend workflow schedules failed");
  }

  public void resume(String namespaceId, String appId, String schedName) throws Exception {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/schedules/%s/resume", getNamespacePath(namespaceId), appId, schedName);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    programLifecycleHttpHandler.performAction(request, responder, namespaceId, appId,
                                              "schedules", schedName, "resume");
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Resume workflow schedules failed");
  }

  public String scheduleStatus(String namespaceId, String appId, String schedId, int expectedResponseCode)
    throws Exception {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/schedules/%s/status", getNamespacePath(namespaceId), appId, schedId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    try {
      programLifecycleHttpHandler.getStatus(request, responder, namespaceId, appId, "schedules", schedId);
    } catch (NotFoundException e) {
      return "NOT_FOUND";
    }
    verifyResponse(HttpResponseStatus.valueOf(expectedResponseCode), responder.getStatus(),
                   "Get schedules status failed");
      Map<String, String> json = responder.decodeResponseContent(MAP_TYPE);
      return json.get("status");
  }

  private void verifyResponse(HttpResponseStatus expected, HttpResponseStatus actual, String errorMsg) {
    if (!expected.equals(actual)) {
      if (actual.getCode() == HttpResponseStatus.FORBIDDEN.getCode()) {
        throw new UnauthorizedException(actual.getReasonPhrase());
      }
      throw new IllegalStateException(String.format("Expected %s, got %s. Error: %s",
                                                    expected, actual, errorMsg));
    }
  }

  public Location deployApplication(Id.Namespace namespace, Class<?> applicationClz, String config,
                                    @Nullable KerberosPrincipalId ownerPrincipal,
                                    File...bundleEmbeddedJars) throws Exception {

    Preconditions.checkNotNull(applicationClz, "Application cannot be null.");

    Location deployedJar = AppJarHelper.createDeploymentJar(locationFactory, applicationClz, bundleEmbeddedJars);
    LOG.info("Created deployedJar at {}", deployedJar);

    String archiveName = String.format("%s-1.0.%d.jar", applicationClz.getSimpleName(), System.currentTimeMillis());
    DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                                                        String.format("/v3/namespaces/%s/apps", namespace.getId()));
    request.setHeader(Constants.Gateway.API_KEY, "api-key-example");
    request.setHeader(AbstractAppFabricHttpHandler.ARCHIVE_NAME_HEADER, archiveName);
    if (config != null) {
      request.setHeader(AbstractAppFabricHttpHandler.APP_CONFIG_HEADER, config);
    }
    String owner = null;
    if (ownerPrincipal != null) {
      owner = GSON.toJson(ownerPrincipal, KerberosPrincipalId.class);
      request.setHeader(AbstractAppFabricHttpHandler.PRINCIPAL_HEADER, owner);
    }
    MockResponder mockResponder = new MockResponder();
    BodyConsumer bodyConsumer = appLifecycleHttpHandler.deploy(request, mockResponder, namespace.getId(), archiveName,
                                                               config, owner, true);
    Preconditions.checkNotNull(bodyConsumer, "BodyConsumer from deploy call should not be null");

    try (BufferFileInputStream is = new BufferFileInputStream(deployedJar.getInputStream(), 100 * 1024)) {
      byte[] chunk = is.read();
      while (chunk.length > 0) {
        mockResponder = new MockResponder();
        bodyConsumer.chunk(ChannelBuffers.wrappedBuffer(chunk), mockResponder);
        Preconditions.checkState(mockResponder.getStatus() == null, "failed to deploy app");
        chunk = is.read();
      }
      mockResponder = new MockResponder();
      bodyConsumer.finished(mockResponder);
      verifyResponse(HttpResponseStatus.OK, mockResponder.getStatus(), "Failed to deploy app");
    }
    return deployedJar;
  }

  public void deployApplication(Id.Application appId, AppRequest appRequest) throws Exception {
    DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT,
      String.format("%s/apps/%s", getNamespacePath(appId.getNamespaceId()), appId.getId()));
    createApplication(appId.toEntityId(), request, appRequest);
  }

  public void deployApplication(ApplicationId appId, AppRequest appRequest) throws Exception {
    DefaultHttpRequest requst = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
      String.format("%s/apps/%s/versions/%s/create", getNamespacePath(appId.getNamespace()),
                    appId.getApplication(), appId.getVersion()));
    createApplication(appId, requst, appRequest);
  }

  private void createApplication(ApplicationId appId, DefaultHttpRequest request, AppRequest appRequest)
    throws Exception {
    request.setHeader(Constants.Gateway.API_KEY, "api-key-example");
    request.setContent(ChannelBuffers.wrappedBuffer(Bytes.toBytes(GSON.toJson(appRequest.getConfig()))));

    MockResponder mockResponder = new MockResponder();
    BodyConsumer bodyConsumer = appLifecycleHttpHandler.createAppVersion(request, mockResponder, appId.getNamespace(),
                                                                         appId.getApplication(), appId.getVersion());
    Preconditions.checkNotNull(bodyConsumer, "BodyConsumer from deploy call should not be null");

    byte[] contents = Bytes.toBytes(GSON.toJson(appRequest));
    Preconditions.checkNotNull(contents);
    bodyConsumer.chunk(ChannelBuffers.wrappedBuffer(contents), mockResponder);
    bodyConsumer.finished(mockResponder);
    verifyResponse(HttpResponseStatus.OK, mockResponder.getStatus(), "Failed to deploy app");
  }

  public void updateApplication(ApplicationId appId, AppRequest appRequest) throws Exception {
    deployApplication(appId, appRequest);
  }

  public void deleteApplication(ApplicationId appId) throws Exception {
    DefaultHttpRequest request = new DefaultHttpRequest(
      HttpVersion.HTTP_1_1, HttpMethod.DELETE,
      String.format("%s/apps/%s/versions/%s", getNamespacePath(appId.getNamespace()), appId.getApplication(),
                    appId.getVersion()));
    request.setHeader(Constants.Gateway.API_KEY, "api-key-example");
    MockResponder mockResponder = new MockResponder();
    appLifecycleHttpHandler.deleteApp(request, mockResponder, appId.getNamespace(), appId.getApplication());
    verifyResponse(HttpResponseStatus.OK, mockResponder.getStatus(), "Deleting app failed");
  }

  public void deleteAllApplications(NamespaceId namespaceId) throws Exception {
    DefaultHttpRequest request = new DefaultHttpRequest(
      HttpVersion.HTTP_1_1, HttpMethod.DELETE,
      String.format("%s/apps", getNamespacePath(namespaceId.getNamespace()))
    );
    request.setHeader(Constants.Gateway.API_KEY, "api-key-example");
    MockResponder mockResponder = new MockResponder();
    appLifecycleHttpHandler.deleteAllApps(request, mockResponder, namespaceId.getNamespace());
    verifyResponse(HttpResponseStatus.OK, mockResponder.getStatus(), "Deleting all apps failed");
  }

  public ApplicationDetail getInfo(ApplicationId appId) throws Exception {
    DefaultHttpRequest request = new DefaultHttpRequest(
      HttpVersion.HTTP_1_1, HttpMethod.GET,
      String.format("%s/apps/%s", getNamespacePath(appId.getNamespace()), appId.getApplication())
    );
    request.setHeader(Constants.Gateway.API_KEY, "api-key-example");
    MockResponder mockResponder = new MockResponder();
    appLifecycleHttpHandler.getAppInfo(request, mockResponder, appId.getNamespace(), appId.getApplication());
    verifyResponse(HttpResponseStatus.OK, mockResponder.getStatus(), "Getting app info failed");
    return mockResponder.decodeResponseContent(new TypeToken<ApplicationDetail>() { }.getType(), GSON);
  }

  public ApplicationDetail getVersionedInfo(ApplicationId appId) throws Exception {
    DefaultHttpRequest request = new DefaultHttpRequest(
      HttpVersion.HTTP_1_1, HttpMethod.GET,
      String.format("%s/apps/%s/versions/%s", getNamespacePath(appId.getNamespace()),
                    appId.getApplication(), appId.getVersion())
    );
    request.setHeader(Constants.Gateway.API_KEY, "api-key-example");
    MockResponder mockResponder = new MockResponder();
    appLifecycleHttpHandler.getAppVersionInfo(request, mockResponder, appId.getNamespace(), appId.getApplication(),
                                              appId.getVersion());
    verifyResponse(HttpResponseStatus.OK, mockResponder.getStatus(), "Getting app version info failed");
    return mockResponder.decodeResponseContent(new TypeToken<ApplicationDetail>() { }.getType(), GSON);
  }

  public Collection<String> listAppVersions(ApplicationId appId) throws Exception {
    DefaultHttpRequest request = new DefaultHttpRequest(
      HttpVersion.HTTP_1_1, HttpMethod.GET,
      String.format("%s/apps/%s/versions", getNamespacePath(appId.getNamespace()), appId.getApplication())
    );
    request.setHeader(Constants.Gateway.API_KEY, "api-key-example");
    MockResponder mockResponder = new MockResponder();
    appLifecycleHttpHandler.listAppVersions(request, mockResponder, appId.getNamespace(), appId.getApplication());
    verifyResponse(HttpResponseStatus.OK, mockResponder.getStatus(), "Failed to list application versions");
    return mockResponder.decodeResponseContent(new TypeToken<Collection<String>>() { }.getType(), GSON);
  }

  public void setRuntimeArgs(ProgramId programId, Map<String, String> args) throws Exception {
    DefaultHttpRequest request = new DefaultHttpRequest(
      HttpVersion.HTTP_1_1, HttpMethod.PUT,
      String.format("%s/apps/%s/%s/%s/runtimeargs", getNamespacePath(programId.getNamespace()),
                    programId.getApplication(), programId.getType().getCategoryName(), programId.getProgram())
    );
    request.setHeader(Constants.Gateway.API_KEY, "api-key-example");
    byte[] contents = Bytes.toBytes(GSON.toJson(args));
    Preconditions.checkNotNull(contents);
    request.setContent(ChannelBuffers.wrappedBuffer(contents));
    MockResponder mockResponder = new MockResponder();
    programLifecycleHttpHandler.saveProgramRuntimeArgs(request, mockResponder, programId.getNamespace(),
                                                       programId.getApplication(),
                                                       programId.getType().getCategoryName(), programId.getProgram());
    verifyResponse(HttpResponseStatus.OK, mockResponder.getStatus(), "Saving runtime arguments failed");
  }

  public Map<String, String> getRuntimeArgs(ProgramId programId) throws Exception {
    DefaultHttpRequest request = new DefaultHttpRequest(
      HttpVersion.HTTP_1_1, HttpMethod.GET,
      String.format("%s/apps/%s/%s/%s/runtimeargs", getNamespacePath(programId.getNamespace()),
                    programId.getApplication(), programId.getType().getCategoryName(), programId.getProgram())
    );
    request.setHeader(Constants.Gateway.API_KEY, "api-key-example");
    MockResponder mockResponder = new MockResponder();
    programLifecycleHttpHandler.getProgramRuntimeArgs(request, mockResponder, programId.getNamespace(),
                                                       programId.getApplication(),
                                                       programId.getType().getCategoryName(), programId.getProgram());
    verifyResponse(HttpResponseStatus.OK, mockResponder.getStatus(), "Getting runtime arguments failed");
    return mockResponder.decodeResponseContent(MAP_TYPE);
  }

  public List<PluginInstanceDetail> getPlugins(ApplicationId application) throws Exception {
    DefaultHttpRequest request = new DefaultHttpRequest(
      HttpVersion.HTTP_1_1, HttpMethod.GET,
      String.format("%s/apps/%s", getNamespacePath(application.getNamespace()), application.getApplication())
    );
    request.setHeader(Constants.Gateway.API_KEY, "api-key-example");
    MockResponder mockResponder = new MockResponder();
    appLifecycleHttpHandler.getPluginsInfo(request, mockResponder, application.getNamespace(),
                                           application.getApplication());
    verifyResponse(HttpResponseStatus.OK, mockResponder.getStatus(), "Getting app info failed");
    return mockResponder.decodeResponseContent(new TypeToken<List<PluginInstanceDetail>>() { }.getType(), GSON);
  }
}
