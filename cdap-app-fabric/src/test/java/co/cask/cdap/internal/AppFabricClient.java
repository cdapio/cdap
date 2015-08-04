/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.AdapterHttpHandler;
import co.cask.cdap.gateway.handlers.AppLifecycleHttpHandler;
import co.cask.cdap.gateway.handlers.NamespaceHttpHandler;
import co.cask.cdap.gateway.handlers.ProgramLifecycleHttpHandler;
import co.cask.cdap.gateway.handlers.WorkflowHttpHandler;
import co.cask.cdap.internal.app.BufferFileInputStream;
import co.cask.cdap.internal.app.namespace.NamespaceAdmin;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.Instances;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.ServiceInstances;
import co.cask.cdap.proto.WorkflowTokenDetail;
import co.cask.cdap.proto.WorkflowTokenNodeDetail;
import co.cask.cdap.proto.codec.ScheduleSpecificationCodec;
import co.cask.cdap.proto.codec.WorkflowTokenDetailCodec;
import co.cask.cdap.proto.codec.WorkflowTokenNodeDetailCodec;
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
    .create();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type RUN_RECORDS_TYPE = new TypeToken<List<RunRecord>>() { }.getType();
  private static final Type SCHEDULES_TYPE = new TypeToken<List<ScheduleSpecification>>() { }.getType();

  private final LocationFactory locationFactory;
  private final AppLifecycleHttpHandler appLifecycleHttpHandler;
  private final ProgramLifecycleHttpHandler programLifecycleHttpHandler;
  private final WorkflowHttpHandler workflowHttpHandler;
  private final NamespaceHttpHandler namespaceHttpHandler;
  private final NamespaceAdmin namespaceAdmin;
  private final AdapterHttpHandler adapterHttpHandler;

  @Inject
  public AppFabricClient(LocationFactory locationFactory,
                         AppLifecycleHttpHandler appLifecycleHttpHandler,
                         ProgramLifecycleHttpHandler programLifecycleHttpHandler,
                         NamespaceHttpHandler namespaceHttpHandler,
                         NamespaceAdmin namespaceAdmin,
                         WorkflowHttpHandler workflowHttpHandler,
                         AdapterHttpHandler adapterHttpHandler) {
    this.locationFactory = locationFactory;
    this.appLifecycleHttpHandler = appLifecycleHttpHandler;
    this.programLifecycleHttpHandler = programLifecycleHttpHandler;
    this.namespaceHttpHandler = namespaceHttpHandler;
    this.namespaceAdmin = namespaceAdmin;
    this.workflowHttpHandler = workflowHttpHandler;
    this.adapterHttpHandler = adapterHttpHandler;
  }

  private String getNamespacePath(String namespaceId) {
    return String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, namespaceId);
  }

  public void reset() throws Exception {
    MockResponder responder;
    HttpRequest request;

    // delete all namespaces
    for (NamespaceMeta namespaceMeta : namespaceAdmin.listNamespaces()) {
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

  public void stopProgram(String namespaceId, String appId, String flowId, ProgramType type) throws Exception {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/%s/%s/stop",
                               getNamespacePath(namespaceId), appId, type.getCategoryName(), flowId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    programLifecycleHttpHandler.performAction(request, responder, namespaceId, appId,
                                              type.getCategoryName(), flowId, "stop");
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Stop " + type + " failed");
  }

  public String getStatus(String namespaceId, String appId, String flowId, ProgramType type) {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/%s/%s/status", getNamespacePath(namespaceId), appId, type, flowId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    programLifecycleHttpHandler.getStatus(request, responder, namespaceId, appId, type.getCategoryName(), flowId);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Get status " + type + " failed");
    Map<String, String> json = responder.decodeResponseContent(MAP_TYPE);
    return json.get("status");
  }

  public void setWorkerInstances(String namespaceId, String appId, String workerId, int instances) {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/worker/%s/instances", getNamespacePath(namespaceId), appId, workerId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, uri);
    JsonObject json = new JsonObject();
    json.addProperty("instances", instances);
    request.setContent(ChannelBuffers.wrappedBuffer(json.toString().getBytes()));
    programLifecycleHttpHandler.setWorkerInstances(request, responder, namespaceId, appId, workerId);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Set worker instances failed");
  }

  public Instances getWorkerInstances(String namespaceId, String appId, String workerId) {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/worker/%s/instances", getNamespacePath(namespaceId), appId, workerId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    programLifecycleHttpHandler.getWorkerInstances(request, responder, namespaceId, appId, workerId);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Get worker instances failed");
    return responder.decodeResponseContent(Instances.class);
  }

  public void setServiceInstances(String namespaceId, String applicationId, String serviceName, int instances) {
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

  public ServiceInstances getServiceInstances(String namespaceId, String applicationId, String serviceName) {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/services/%s/instances",
                               getNamespacePath(namespaceId), applicationId, serviceName);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    programLifecycleHttpHandler.getServiceInstances(request, responder, namespaceId, applicationId, serviceName);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Get service instances failed");
    return responder.decodeResponseContent(ServiceInstances.class);
  }

  public void setFlowletInstances(String namespaceId, String applicationId,
                                  String flowId, String flowletName, int instances) {
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

  public Instances getFlowletInstances(String namespaceId, String applicationId, String flowName, String flowletName) {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/flows/%s/flowlets/%s/instances",
                               getNamespacePath(namespaceId), applicationId, flowName, flowletName);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    programLifecycleHttpHandler.getFlowletInstances(request, responder, namespaceId, applicationId, flowName,
                                                    flowletName);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Get flowlet instances failed");
    return responder.decodeResponseContent(Instances.class);
  }

  public List<ScheduleSpecification> getSchedules(String namespaceId, String appId, String wflowId) {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/workflows/%s/schedules",
                               getNamespacePath(namespaceId), appId, wflowId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    workflowHttpHandler.getWorkflowSchedules(request, responder, namespaceId, appId, wflowId);

    List<ScheduleSpecification> schedules = responder.decodeResponseContent(SCHEDULES_TYPE, GSON);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Getting workflow schedules failed");
    return schedules;
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

  public List<RunRecord> getHistory(String namespaceId, String appId, String wflowId) {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/workflows/%s/runs?status=completed",
                               getNamespacePath(namespaceId), appId, wflowId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    programLifecycleHttpHandler.programHistory(request, responder, namespaceId, appId,
                                               "workflows", wflowId, null, null, null, 100);
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

  public String scheduleStatus(String namespaceId, String appId, String schedId, int expectedResponseCode) {
    MockResponder responder = new MockResponder();
    String uri = String.format("%s/apps/%s/schedules/%s/status", getNamespacePath(namespaceId), appId, schedId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    programLifecycleHttpHandler.getStatus(request, responder, namespaceId, appId, "schedules", schedId);
    verifyResponse(HttpResponseStatus.valueOf(expectedResponseCode), responder.getStatus(),
                   "Get schedules status failed");
    if (HttpResponseStatus.NOT_FOUND.getCode() == expectedResponseCode) {
      return "NOT_FOUND";
    } else {
      Map<String, String> json = responder.decodeResponseContent(MAP_TYPE);
      return json.get("status");
    }
  }

  private void verifyResponse(HttpResponseStatus expected, HttpResponseStatus actual, String errorMsg) {
    if (!expected.equals(actual)) {
      throw new IllegalStateException(String.format("Expected %s, got %s. Error: %s",
                                                    expected, actual, errorMsg));
    }
  }

  public Location deployApplication(Id.Namespace namespace, String appName, Class<?> applicationClz,
                                    File ...bundleEmbeddedJars) throws Exception {
    return deployApplication(namespace, appName, applicationClz, null, bundleEmbeddedJars);
  }

  public Location deployApplication(Id.Namespace namespace, String appName, Class<?> applicationClz,
                                    String config, File...bundleEmbeddedJars) throws Exception {

    Preconditions.checkNotNull(applicationClz, "Application cannot be null.");

    Location deployedJar = AppJarHelper.createDeploymentJar(locationFactory, applicationClz, bundleEmbeddedJars);
    LOG.info("Created deployedJar at {}", deployedJar.toURI().toASCIIString());

    String archiveName = appName + ".jar";
    DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                                                        String.format("/v3/namespaces/%s/apps", namespace.getId()));
    request.setHeader(Constants.Gateway.API_KEY, "api-key-example");
    request.setHeader("X-Archive-Name", archiveName);
    if (config != null) {
      request.setHeader("X-App-Config", config);
    }
    MockResponder mockResponder = new MockResponder();
    BodyConsumer bodyConsumer = appLifecycleHttpHandler.deploy(request, mockResponder, namespace.getId(), archiveName,
                                                               config);
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

  public void deployTemplate(Id.Namespace namespace, Id.ApplicationTemplate templateId) {
    DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT,
      String.format("%s/templates/%s", getNamespacePath(namespace.getId()), templateId.getId()));
    MockResponder responder = new MockResponder();

    try {
      adapterHttpHandler.deployTemplate(request, responder, namespace.getId(), templateId.getId());
      verifyResponse(HttpResponseStatus.valueOf(200), responder.getStatus(), "Failed to deploy template.");
    } catch (Exception e) {
      LOG.error("Error deploying template", e);
    }
  }

  public void createAdapter(Id.Adapter id, AdapterConfig config) {
    DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT,
      String.format("%s/adapters/%s", getNamespacePath(id.getNamespaceId()), id.getId()));
    request.setContent(ChannelBuffers.wrappedBuffer(GSON.toJson(config).getBytes(Charsets.UTF_8)));
    MockResponder responder = new MockResponder();

    try {
      adapterHttpHandler.createAdapter(request, responder, id.getNamespaceId(), id.getId());
      verifyResponse(HttpResponseStatus.valueOf(200), responder.getStatus(), "Failed to create adapter.");
    } catch (Exception e) {
      LOG.error("Error creating adapter", e);
    }
  }

  public void startAdapter(Id.Adapter id) {
    DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
      String.format("%s/adapters/%s/start", getNamespacePath(id.getNamespaceId()), id.getId()));
    MockResponder responder = new MockResponder();

    try {
      adapterHttpHandler.startStopAdapter(request, responder, id.getNamespaceId(), id.getId(), "start");
      verifyResponse(HttpResponseStatus.valueOf(200), responder.getStatus(), "Failed to start adapter.");
    } catch (Exception e) {
      LOG.error("Error starting adapter", e);
    }
  }

  public void stopAdapter(Id.Adapter id) {
    DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
      String.format("%s/adapters/%s/stop", getNamespacePath(id.getNamespaceId()), id.getId()));
    MockResponder responder = new MockResponder();

    try {
      adapterHttpHandler.startStopAdapter(request, responder, id.getNamespaceId(), id.getId(), "stop");
      verifyResponse(HttpResponseStatus.valueOf(200), responder.getStatus(), "Failed to stop adapter.");
    } catch (Exception e) {
      LOG.error("Error stopping adapter", e);
    }
  }

  public List<RunRecord> getAdapterRuns(Id.Adapter id) {
    DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
      String.format("%s/adapters/%s/runs", getNamespacePath(id.getNamespaceId()), id.getId()));
    MockResponder responder = new MockResponder();

    try {
      adapterHttpHandler.getAdapterRuns(request, responder, id.getNamespaceId(), id.getId(), null, null, null, 100);
      verifyResponse(HttpResponseStatus.valueOf(200), responder.getStatus(), "Failed to get runs for adapter.");
      return responder.decodeResponseContent(RUN_RECORDS_TYPE);
    } catch (Exception e) {
      LOG.error("Error getting adapter runs", e);
      throw Throwables.propagate(e);
    }
  }

  public RunRecord getAdapterRun(Id.Adapter adapterId, String runId) {
    DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
      String.format("%s/adapters/%s/runs/%s", getNamespacePath(adapterId.getNamespaceId()), adapterId.getId(), runId));
    MockResponder responder = new MockResponder();

    try {
      adapterHttpHandler.getAdapterRun(request, responder, adapterId.getNamespaceId(), adapterId.getId(), runId);
      verifyResponse(HttpResponseStatus.valueOf(200), responder.getStatus(), "Failed to get run for adapter.");
      return responder.decodeResponseContent(RunRecord.class);
    } catch (Exception e) {
      LOG.error("Error getting adapter run", e);
      throw Throwables.propagate(e);
    }
  }

  public void deleteAdapter(Id.Adapter adapterId) throws Exception {
    String url = String.format("%s/adapters/%s", getNamespacePath(adapterId.getNamespaceId()), adapterId.getId());
    DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.DELETE, url);
    MockResponder responder = new MockResponder();
    adapterHttpHandler.deleteAdapter(request, responder, adapterId.getNamespaceId(), adapterId.getId());
    verifyResponse(HttpResponseStatus.valueOf(200), responder.getStatus(), "Failed to delete adapter.");
  }
}
