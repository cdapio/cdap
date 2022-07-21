/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.gateway.handlers.AppLifecycleHttpHandler;
import io.cdap.cdap.gateway.handlers.NamespaceHttpHandler;
import io.cdap.cdap.gateway.handlers.ProgramLifecycleHttpHandler;
import io.cdap.cdap.gateway.handlers.WorkflowHttpHandler;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.internal.app.BufferFileInputStream;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.Instances;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.PluginInstanceDetail;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.ProtoConstraintCodec;
import io.cdap.cdap.proto.ProtoTrigger;
import io.cdap.cdap.proto.ProtoTriggerCodec;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.ServiceInstances;
import io.cdap.cdap.proto.WorkflowNodeStateDetail;
import io.cdap.cdap.proto.WorkflowTokenDetail;
import io.cdap.cdap.proto.WorkflowTokenNodeDetail;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.codec.WorkflowTokenDetailCodec;
import io.cdap.cdap.proto.codec.WorkflowTokenNodeDetailCodec;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.BodyConsumer;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Client tool for AppFabricHttpHandler.
 */
public class AppFabricClient {
  private static final Logger LOG = LoggerFactory.getLogger(AppFabricClient.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(WorkflowTokenDetail.class, new WorkflowTokenDetailCodec())
    .registerTypeAdapter(WorkflowTokenNodeDetail.class, new WorkflowTokenNodeDetailCodec())
    .registerTypeAdapter(Trigger.class, new ProtoTriggerCodec())
    .registerTypeAdapter(ProtoTrigger.class, new ProtoTriggerCodec())
    .registerTypeAdapter(Constraint.class, new ProtoConstraintCodec())
    .create();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type RUN_RECORDS_TYPE = new TypeToken<List<RunRecord>>() { }.getType();
  private static final Type SCHEDULE_DETAILS_TYPE = new TypeToken<List<ScheduleDetail>>() { }.getType();

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
      request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.DELETE, String.format(
        "%s/unrecoverable/namespaces/%s/datasets", Constants.Gateway.API_VERSION_3, namespace.getId()));
      namespaceHttpHandler.deleteDatasets(request, responder, namespaceMeta.getName());
      verifyResponse(HttpResponseStatus.OK, responder.getStatus(),
                     String.format("could not delete datasets in namespace '%s'", namespace.getId()));

      responder = new MockResponder();
      request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.DELETE,
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
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    String argString = GSON.toJson(args);
    if (argString != null) {
      request.content().writeCharSequence(argString, StandardCharsets.UTF_8);
    }
    HttpUtil.setContentLength(request, request.content().readableBytes());
    programLifecycleHttpHandler.performAction(request, responder, namespaceId, appId,
                                              type.getCategoryName(), flowId, "start");
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Start " + type + " failed");
  }

  public void startProgram(String namespaceId, String appId, String appVersion, String programId,
                           ProgramType type, Map<String, String> args) throws Exception {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/versions/%s/%s/%s/start", getNamespacePath(namespaceId), appId, appVersion,
                               type.getCategoryName(), programId);
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    String argString = GSON.toJson(args);
    if (argString != null) {
      request.content().writeCharSequence(argString, StandardCharsets.UTF_8);
    }
    HttpUtil.setContentLength(request, request.content().readableBytes());
    programLifecycleHttpHandler.performAction(request, responder, namespaceId, appId, appVersion,
                                              type.getCategoryName(), programId, "start");
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Start " + type + " failed");
  }


  public void stopProgram(String namespaceId, String appId, String programId, ProgramType type) throws Exception {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/%s/%s/stop",
                               getNamespacePath(namespaceId), appId, type.getCategoryName(), programId);
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    HttpUtil.setContentLength(request, request.content().readableBytes());
    programLifecycleHttpHandler.performAction(request, responder, namespaceId, appId,
                                              type.getCategoryName(), programId, "stop");
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Stop " + type + " failed");
  }

  public void stopProgram(String namespaceId, String appId, String appVersion, String programId,
                          ProgramType type) throws Exception {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/versions/%s/%s/%s/stop",
                               getNamespacePath(namespaceId), appId, appVersion, type.getCategoryName(), programId);
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    HttpUtil.setContentLength(request, request.content().readableBytes());
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
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, uri);
    JsonObject json = new JsonObject();
    json.addProperty("instances", instances);
    request.content().writeCharSequence(json.toString(), StandardCharsets.UTF_8);
    HttpUtil.setContentLength(request, request.content().readableBytes());
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
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, uri);
    JsonObject json = new JsonObject();
    json.addProperty("instances", instances);
    request.content().writeCharSequence(json.toString(), StandardCharsets.UTF_8);
    HttpUtil.setContentLength(request, request.content().readableBytes());
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

  public List<ScheduleDetail> getProgramSchedules(String namespace, String app, String workflow)
    throws NotFoundException, UnauthorizedException {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/workflows/%s/schedules",
                               getNamespacePath(namespace), app, workflow);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    try {
      programLifecycleHttpHandler.getProgramSchedules(request, responder, namespace, app, workflow, null, null, null);
    } catch (Exception e) {
      // cannot happen
      throw Throwables.propagate(e);
    }

    List<ScheduleDetail> schedules = responder.decodeResponseContent(SCHEDULE_DETAILS_TYPE, GSON);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Getting workflow schedules failed");
    return schedules;
  }

  public WorkflowTokenDetail getWorkflowToken(String namespaceId, String appId, String wflowId, String runId,
                                              @Nullable WorkflowToken.Scope scope,
                                              @Nullable String key) throws NotFoundException, UnauthorizedException {
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
                                                  @Nullable String key)
    throws NotFoundException, UnauthorizedException {
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
    throws NotFoundException, UnauthorizedException {
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
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    HttpUtil.setContentLength(request, request.content().readableBytes());
    programLifecycleHttpHandler.performAction(request, responder, namespaceId, appId,
                                              "schedules", scheduleName, "suspend");
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Suspend workflow schedules failed");
  }

  public void resume(String namespaceId, String appId, String schedName) throws Exception {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/schedules/%s/resume", getNamespacePath(namespaceId), appId, schedName);
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    HttpUtil.setContentLength(request, request.content().readableBytes());
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

  private void verifyResponse(HttpResponseStatus expected, HttpResponseStatus actual, String errorMsg)
    throws UnauthorizedException {
    if (!expected.equals(actual)) {
      if (actual.code() == HttpResponseStatus.FORBIDDEN.code()) {
        throw new UnauthorizedException(errorMsg + ": " + actual.reasonPhrase());
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
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                                                         String.format("/v3/namespaces/%s/apps", namespace.getId()));
    request.headers().set(Constants.Gateway.API_KEY, "api-key-example");
    request.headers().set(AbstractAppFabricHttpHandler.ARCHIVE_NAME_HEADER, archiveName);
    if (config != null) {
      request.headers().set(AbstractAppFabricHttpHandler.APP_CONFIG_HEADER, config);
    }
    String owner = null;
    if (ownerPrincipal != null) {
      owner = GSON.toJson(ownerPrincipal, KerberosPrincipalId.class);
      request.headers().set(AbstractAppFabricHttpHandler.PRINCIPAL_HEADER, owner);
    }
    MockResponder mockResponder = new MockResponder();
    BodyConsumer bodyConsumer = appLifecycleHttpHandler.deploy(request, mockResponder, namespace.getId(), archiveName,
                                                               config, owner, true);
    Preconditions.checkNotNull(bodyConsumer, "BodyConsumer from deploy call should not be null");

    try (BufferFileInputStream is = new BufferFileInputStream(deployedJar.getInputStream(), 100 * 1024)) {
      byte[] chunk = is.read();
      while (chunk.length > 0) {
        mockResponder = new MockResponder();
        bodyConsumer.chunk(Unpooled.wrappedBuffer(chunk), mockResponder);
        Preconditions.checkState(mockResponder.getStatus() == null, "failed to deploy app");
        chunk = is.read();
      }
      mockResponder = new MockResponder();
      bodyConsumer.finished(mockResponder);
      verifyResponse(HttpResponseStatus.OK, mockResponder.getStatus(), "Failed to deploy app (" +
        mockResponder.getResponseContentAsString() + ")");
    }
    return deployedJar;
  }

  public void deployApplication(Id.Application appId, AppRequest appRequest) throws Exception {
    deployApplication(appId.toEntityId(), appRequest);
  }

  public void deployApplication(ApplicationId appId, AppRequest appRequest) throws Exception {
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
      String.format("%s/apps/%s/versions/%s/create", getNamespacePath(appId.getNamespace()),
                    appId.getApplication(), appId.getVersion()));

    request.headers().set(Constants.Gateway.API_KEY, "api-key-example");
    HttpUtil.setTransferEncodingChunked(request, true);

    MockResponder mockResponder = new MockResponder();
    BodyConsumer bodyConsumer = appLifecycleHttpHandler.createAppVersion(request, mockResponder, appId.getNamespace(),
                                                                         appId.getApplication(), appId.getVersion());
    Preconditions.checkNotNull(bodyConsumer, "BodyConsumer from deploy call should not be null");

    bodyConsumer.chunk(Unpooled.copiedBuffer(GSON.toJson(appRequest), StandardCharsets.UTF_8), mockResponder);
    bodyConsumer.finished(mockResponder);
    verifyResponse(HttpResponseStatus.OK, mockResponder.getStatus(), "Failed to deploy app (" +
      mockResponder.getResponseContentAsString() + ")");
  }

  public void updateApplication(ApplicationId appId, AppRequest appRequest) throws Exception {
    deployApplication(appId, appRequest);
  }

  public void deleteApplication(ApplicationId appId) throws Exception {
    HttpRequest request = new DefaultHttpRequest(
      HttpVersion.HTTP_1_1, HttpMethod.DELETE,
      String.format("%s/apps/%s/versions/%s", getNamespacePath(appId.getNamespace()), appId.getApplication(),
                    appId.getVersion()));
    request.headers().set(Constants.Gateway.API_KEY, "api-key-example");
    MockResponder mockResponder = new MockResponder();
    appLifecycleHttpHandler.deleteApp(request, mockResponder, appId.getNamespace(), appId.getApplication());
    verifyResponse(HttpResponseStatus.OK, mockResponder.getStatus(), "Deleting app failed");
  }

  public void deleteAllApplications(NamespaceId namespaceId) throws Exception {
    HttpRequest request = new DefaultHttpRequest(
      HttpVersion.HTTP_1_1, HttpMethod.DELETE,
      String.format("%s/apps", getNamespacePath(namespaceId.getNamespace()))
    );
    request.headers().set(Constants.Gateway.API_KEY, "api-key-example");
    MockResponder mockResponder = new MockResponder();
    appLifecycleHttpHandler.deleteAllApps(request, mockResponder, namespaceId.getNamespace());
    verifyResponse(HttpResponseStatus.OK, mockResponder.getStatus(), "Deleting all apps failed");
  }

  public ApplicationDetail getInfo(ApplicationId appId) throws Exception {
    HttpRequest request = new DefaultHttpRequest(
      HttpVersion.HTTP_1_1, HttpMethod.GET,
      String.format("%s/apps/%s", getNamespacePath(appId.getNamespace()), appId.getApplication())
    );
    request.headers().set(Constants.Gateway.API_KEY, "api-key-example");
    MockResponder mockResponder = new MockResponder();
    appLifecycleHttpHandler.getAppInfo(request, mockResponder, appId.getNamespace(), appId.getApplication());
    verifyResponse(HttpResponseStatus.OK, mockResponder.getStatus(), "Getting app info failed");
    return mockResponder.decodeResponseContent(new TypeToken<ApplicationDetail>() { }.getType(), GSON);
  }

  public ApplicationDetail getVersionedInfo(ApplicationId appId) throws Exception {
    HttpRequest request = new DefaultHttpRequest(
      HttpVersion.HTTP_1_1, HttpMethod.GET,
      String.format("%s/apps/%s/versions/%s", getNamespacePath(appId.getNamespace()),
                    appId.getApplication(), appId.getVersion())
    );
    request.headers().set(Constants.Gateway.API_KEY, "api-key-example");
    MockResponder mockResponder = new MockResponder();
    appLifecycleHttpHandler.getAppVersionInfo(request, mockResponder, appId.getNamespace(), appId.getApplication(),
                                              appId.getVersion());
    verifyResponse(HttpResponseStatus.OK, mockResponder.getStatus(), "Getting app version info failed");
    return mockResponder.decodeResponseContent(new TypeToken<ApplicationDetail>() { }.getType(), GSON);
  }

  public Collection<String> listAppVersions(ApplicationId appId) throws Exception {
    HttpRequest request = new DefaultHttpRequest(
      HttpVersion.HTTP_1_1, HttpMethod.GET,
      String.format("%s/apps/%s/versions", getNamespacePath(appId.getNamespace()), appId.getApplication())
    );
    request.headers().set(Constants.Gateway.API_KEY, "api-key-example");
    MockResponder mockResponder = new MockResponder();
    appLifecycleHttpHandler.listAppVersions(request, mockResponder, appId.getNamespace(), appId.getApplication());
    verifyResponse(HttpResponseStatus.OK, mockResponder.getStatus(), "Failed to list application versions");
    return mockResponder.decodeResponseContent(new TypeToken<Collection<String>>() { }.getType(), GSON);
  }

  public void setRuntimeArgs(ProgramId programId, Map<String, String> args) throws Exception {
    FullHttpRequest request = new DefaultFullHttpRequest(
      HttpVersion.HTTP_1_1, HttpMethod.PUT,
      String.format("%s/apps/%s/%s/%s/runtimeargs", getNamespacePath(programId.getNamespace()),
                    programId.getApplication(), programId.getType().getCategoryName(), programId.getProgram())
    );
    request.headers().set(Constants.Gateway.API_KEY, "api-key-example");
    request.content().writeCharSequence(GSON.toJson(args), StandardCharsets.UTF_8);
    HttpUtil.setContentLength(request, request.content().readableBytes());
    MockResponder mockResponder = new MockResponder();
    programLifecycleHttpHandler.saveProgramRuntimeArgs(request, mockResponder, programId.getNamespace(),
                                                       programId.getApplication(),
                                                       programId.getType().getCategoryName(), programId.getProgram());
    verifyResponse(HttpResponseStatus.OK, mockResponder.getStatus(), "Saving runtime arguments failed");
  }

  public Map<String, String> getRuntimeArgs(ProgramId programId) throws Exception {
    HttpRequest request = new DefaultHttpRequest(
      HttpVersion.HTTP_1_1, HttpMethod.GET,
      String.format("%s/apps/%s/%s/%s/runtimeargs", getNamespacePath(programId.getNamespace()),
                    programId.getApplication(), programId.getType().getCategoryName(), programId.getProgram())
    );
    request.headers().set(Constants.Gateway.API_KEY, "api-key-example");
    MockResponder mockResponder = new MockResponder();
    programLifecycleHttpHandler.getProgramRuntimeArgs(request, mockResponder, programId.getNamespace(),
                                                       programId.getApplication(),
                                                       programId.getType().getCategoryName(), programId.getProgram());
    verifyResponse(HttpResponseStatus.OK, mockResponder.getStatus(), "Getting runtime arguments failed");
    return mockResponder.decodeResponseContent(MAP_TYPE);
  }

  public List<PluginInstanceDetail> getPlugins(ApplicationId application) throws Exception {
    HttpRequest request = new DefaultHttpRequest(
      HttpVersion.HTTP_1_1, HttpMethod.GET,
      String.format("%s/apps/%s", getNamespacePath(application.getNamespace()), application.getApplication())
    );
    request.headers().set(Constants.Gateway.API_KEY, "api-key-example");
    MockResponder mockResponder = new MockResponder();
    appLifecycleHttpHandler.getPluginsInfo(request, mockResponder, application.getNamespace(),
                                           application.getApplication());
    verifyResponse(HttpResponseStatus.OK, mockResponder.getStatus(), "Getting app info failed");
    return mockResponder.decodeResponseContent(new TypeToken<List<PluginInstanceDetail>>() { }.getType(), GSON);
  }

  public void addSchedule(ApplicationId application, ScheduleDetail scheduleDetail) throws Exception {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/versions/%s/schedules/%s", getNamespacePath(application.getNamespace()),
                               application.getApplication(), application.getVersion(), scheduleDetail.getName());
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, uri);
    request.content().writeCharSequence(GSON.toJson(scheduleDetail), StandardCharsets.UTF_8);
    HttpUtil.setContentLength(request, request.content().readableBytes());
    programLifecycleHttpHandler.addSchedule(request, responder, application.getNamespace(),
                                            application.getApplication(), application.getVersion(),
                                            scheduleDetail.getName());
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Add schedule failed");
  }

  public void enableSchedule(ScheduleId scheduleId) throws Exception {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/versions/%s/program-type/schedules/program-id/%s/action/enable",
                               getNamespacePath(scheduleId.getNamespace()), scheduleId.getVersion(),
                               scheduleId.getApplication(), scheduleId.getSchedule());
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    HttpUtil.setContentLength(request, 0);
    programLifecycleHttpHandler.performAction(request, responder, scheduleId.getNamespace(),
                                              scheduleId.getApplication(), scheduleId.getVersion(),
                                              "schedules", scheduleId.getSchedule(), "enable");
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Enable schedule failed");
  }

  public void updateSchedule(ScheduleId scheduleId, ScheduleDetail scheduleDetail) throws Exception {
    MockResponder responder = new MockResponder();
    ApplicationId application = scheduleId.getParent();
    String uri = String.format("%s/apps/%s/versions/%s/schedules/%s/update",
                               getNamespacePath(application.getNamespace()), application.getApplication(),
                               application.getVersion(), scheduleId.getSchedule());
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    request.content().writeCharSequence(GSON.toJson(scheduleDetail), StandardCharsets.UTF_8);
    HttpUtil.setContentLength(request, request.content().readableBytes());
    programLifecycleHttpHandler.updateSchedule(request, responder, application.getNamespace(),
                                               application.getApplication(), application.getVersion(),
                                               scheduleId.getSchedule());
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Update schedule failed");
  }

  public void deleteSchedule(ScheduleId scheduleId) throws Exception {
    MockResponder responder = new MockResponder();
    ApplicationId application = scheduleId.getParent();
    String uri = String.format("%s/apps/%s/versions/%s/schedules/%s", getNamespacePath(application.getNamespace()),
                               application.getApplication(), application.getVersion(), scheduleId.getSchedule());
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.DELETE, uri);
    programLifecycleHttpHandler.deleteSchedule(request, responder, application.getNamespace(),
                                               application.getApplication(), application.getVersion(),
                                               scheduleId.getSchedule());
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Delete schedule failed");
  }

  public void upgradeApplication(ApplicationId appId) throws Exception {
    upgradeApplication(appId, Collections.emptySet(), false);
  }

  public void upgradeApplication(ApplicationId appId, Set<String> artifactScopes, boolean allowSnapshot)
    throws Exception {
    HttpRequest request = new DefaultHttpRequest(
        HttpVersion.HTTP_1_1, HttpMethod.POST,
        String.format("%s/apps/%s/upgrade", getNamespacePath(appId.getNamespace()), appId.getApplication())
    );
    request.headers().set(Constants.Gateway.API_KEY, "api-key-example");
    HttpUtil.setTransferEncodingChunked(request, true);

    MockResponder mockResponder = new MockResponder();
    appLifecycleHttpHandler.upgradeApplication(request, mockResponder, appId.getNamespace(),
                                               appId.getApplication(), artifactScopes,
                                               allowSnapshot);
    verifyResponse(HttpResponseStatus.OK, mockResponder.getStatus(), "Failed to upgrade app");
  }
}
