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

package co.cask.cdap.test.internal;

import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.ApplicationBundler;
import co.cask.cdap.gateway.handlers.AppFabricHttpHandler;
import co.cask.cdap.gateway.handlers.ServiceHttpHandler;
import co.cask.cdap.internal.app.BufferFileInputStream;
import co.cask.cdap.internal.app.ScheduleSpecificationCodec;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.ServiceInstances;
import co.cask.http.BodyConsumer;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Client tool for AppFabricHttpHandler.
 */
public class AppFabricClient {
  private static final Logger LOG = LoggerFactory.getLogger(AppFabricClient.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ScheduleSpecification.class, new ScheduleSpecificationCodec())
    .create();

  private final AppFabricHttpHandler httpHandler;
  private final ServiceHttpHandler serviceHttpHandler;
  private final LocationFactory locationFactory;

  public AppFabricClient(AppFabricHttpHandler httpHandler, ServiceHttpHandler serviceHttpHandler,
                         LocationFactory locationFactory) {
    this.httpHandler = httpHandler;
    this.serviceHttpHandler = serviceHttpHandler;
    this.locationFactory = locationFactory;
  }

  public void reset() {
    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/unrecoverable/reset");
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, uri);
    httpHandler.resetCDAP(request, responder);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Reset application failed");
  }

  public void startProgram(String appId, String flowId, ProgramType type, Map<String, String> args) {

    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/apps/%s/%s/%s/start", appId, type.getCategoryName(), flowId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    String argString = GSON.toJson(args);
    if (argString != null) {
      request.setContent(ChannelBuffers.wrappedBuffer(argString.getBytes(Charsets.UTF_8)));
    }
    httpHandler.startProgram(request, responder, appId, type.getCategoryName(), flowId);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Start " + type + " failed");
  }

  public void stopProgram(String appId, String flowId, ProgramType type) {

    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/apps/%s/%s/%s/stop", appId, type, flowId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    httpHandler.stopProgram(request, responder, appId, type.getCategoryName(), flowId);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Stop " + type + " failed");
  }

  public String getStatus(String appId, String flowId, ProgramType type) {

    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/apps/%s/%s/%s/status", appId, type, flowId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    httpHandler.getStatus(request, responder, appId, type.getCategoryName(), flowId);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Get status " + type + " failed");
    Map<String, String> json = responder.decodeResponseContent(new TypeToken<Map<String, String>>() { });
    return json.get("status");
  }

  public void setWorkerInstances(String appId, String workerId, int instances) {
    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/apps/%s/worker/%s/instances", appId, workerId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, uri);
    JsonObject json = new JsonObject();
    json.addProperty("instances", instances);
    request.setContent(ChannelBuffers.wrappedBuffer(json.toString().getBytes()));
    httpHandler.setWorkerInstances(request, responder, appId, workerId);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Set worker instances failed");
  }

  public void setRunnableInstances(String applicationId, String serviceName, String runnableName, int instances) {
    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/apps/%s/services/%s/runnables/%s/instances",
                               applicationId, serviceName, runnableName);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, uri);
    JsonObject json = new JsonObject();
    json.addProperty("instances", instances);
    request.setContent(ChannelBuffers.wrappedBuffer(json.toString().getBytes()));
    serviceHttpHandler.setInstances(request, responder, applicationId, serviceName, runnableName);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Set runnable instances failed");
  }

  public ServiceInstances getRunnableInstances(String applicationId, String serviceName, String runnableName) {

    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/apps/%s/services/%s/runnables/%s/instances",
                               applicationId, serviceName, runnableName);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    serviceHttpHandler.getInstances(request, responder, applicationId, serviceName, runnableName);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Get runnable instances failed");
    return responder.decodeResponseContent(new TypeToken<ServiceInstances>() { });
  }

  public void setFlowletInstances(String applicationId, String flowId, String flowletName, int instances) {

    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/apps/%s/flows/%s/flowlets/%s/instances/%s",
                               applicationId, flowId, flowletName, instances);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, uri);
    JsonObject json = new JsonObject();
    json.addProperty("instances", instances);
    request.setContent(ChannelBuffers.wrappedBuffer(json.toString().getBytes()));
    httpHandler.setFlowletInstances(request, responder, applicationId, flowId, flowletName);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Set flowlet instances failed");
  }

  public List<ScheduleSpecification> getSchedules(String appId, String wflowId) {
    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/apps/%s/workflows/%s/schedules", appId, wflowId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    httpHandler.getWorkflowSchedules(request, responder, appId, wflowId);

    List<ScheduleSpecification> schedules = responder.decodeResponseContent(
      new TypeToken<List<ScheduleSpecification>>() { }, GSON);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Getting workflow schedules failed");
    return schedules;
  }

  public List<RunRecord> getHistory(String appId, String wflowId) {
    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/apps/%s/workflows/%s/runs?status=completed", appId, wflowId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    httpHandler.runnableHistory(request, responder, appId, "workflows", wflowId, null, null , null, 100);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Getting workflow history failed");

    return responder.decodeResponseContent(new TypeToken<List<RunRecord>>() { });
  }

  public void suspend(String appId, String scheduleName) {
    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/apps/%s/schedules/%s/suspend", appId, scheduleName);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    httpHandler.suspendSchedule(request, responder, appId, scheduleName);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Suspend workflow schedules failed");
  }

  public void resume(String appId, String schedName) {
    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/apps/%s/schedules/%s/resume", appId, schedName);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    httpHandler.resumeSchedule(request, responder, appId, schedName);
    verifyResponse(HttpResponseStatus.OK, responder.getStatus(), "Resume workflow schedules failed");
  }

  public String scheduleStatus(String appId, String schedId, int expectedResponseCode) {
    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/apps/%s/schedules/%s/status", appId, schedId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    httpHandler.getStatus(request, responder, appId, "schedules", schedId);
    verifyResponse(HttpResponseStatus.valueOf(expectedResponseCode), responder.getStatus(),
                   "Get schedules status failed");
    if (HttpResponseStatus.NOT_FOUND.getCode() == expectedResponseCode) {
      return "NOT_FOUND";
    } else {
      Map<String, String> json = responder.decodeResponseContent(new TypeToken<Map<String, String>>() { });
      return json.get("status");
    }
  }

  private void verifyResponse(HttpResponseStatus expected, HttpResponseStatus actual, String errorMsg) {
    if (!expected.equals(actual)) {
      throw new IllegalStateException(String.format("Expected %s, got %s. Error: %s",
                                                    expected, actual, errorMsg));
    }
  }

  /**
   * Given a class generates a manifest file with main-class as class.
   *
   * @param klass to set as Main-Class in manifest file.
   * @return An instance {@link java.util.jar.Manifest}
   */
  public static Manifest getManifestWithMainClass(Class<?> klass) {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(ManifestFields.MAIN_CLASS, klass.getName());
    return manifest;
  }

  public Location deployApplication(final String applicationId, Class<?> applicationClz, File...bundleEmbeddedJars)
    throws Exception {

    Preconditions.checkNotNull(applicationClz, "Application cannot be null.");

    Location deployedJar =
      locationFactory.create(createDeploymentJar(locationFactory, applicationClz, bundleEmbeddedJars).toURI());
    LOG.info("Created deployedJar at {}", deployedJar.toURI().toASCIIString());

    String archiveName = applicationId + ".jar";
    DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/v2/apps");
    request.setHeader(Constants.Gateway.API_KEY, "api-key-example");
    request.setHeader("X-Archive-Name", archiveName);
    MockResponder mockResponder = new MockResponder();
    BodyConsumer bodyConsumer = httpHandler.deploy(request, mockResponder, archiveName);
    Preconditions.checkNotNull(bodyConsumer, "BodyConsumer from deploy call should not be null");

    BufferFileInputStream is = new BufferFileInputStream(deployedJar.getInputStream(), 100 * 1024);
    try {
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
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      is.close();
    }
    return deployedJar;
  }

  public static File createDeploymentJar(LocationFactory locationFactory, Class<?> clz, Manifest manifest,
                                         File...bundleEmbeddedJars) throws IOException {

    ApplicationBundler bundler = new ApplicationBundler(ImmutableList.of("co.cask.cdap.api",
                                                                         "org.apache.hadoop",
                                                                         "org.apache.hive",
                                                                         "org.apache.spark"),
                                                        ImmutableList.of("org.apache.hadoop.hbase"));
    Location jarLocation = locationFactory.create(clz.getName()).getTempFile(".jar");
    bundler.createBundle(jarLocation, clz);

    Location deployJar = locationFactory.create(clz.getName()).getTempFile(".jar");

    // Create the program jar for deployment. It removes the "classes/" prefix as that's the convention taken
    // by the ApplicationBundler inside Twill.
    JarOutputStream jarOutput = new JarOutputStream(deployJar.getOutputStream(), manifest);
    try {
      JarInputStream jarInput = new JarInputStream(jarLocation.getInputStream());
      try {
        JarEntry jarEntry = jarInput.getNextJarEntry();
        while (jarEntry != null) {
          boolean isDir = jarEntry.isDirectory();
          String entryName = jarEntry.getName();
          if (!entryName.equals("classes/")) {
            if (entryName.startsWith("classes/")) {
              jarEntry = new JarEntry(entryName.substring("classes/".length()));
            } else {
              jarEntry = new JarEntry(entryName);
            }

            // TODO: this is due to manifest possibly already existing in the jar, but we also
            // create a manifest programatically so it's possible to have a duplicate entry here
            if ("META-INF/MANIFEST.MF".equalsIgnoreCase(jarEntry.getName())) {
              jarEntry = jarInput.getNextJarEntry();
              continue;
            }

            jarOutput.putNextEntry(jarEntry);
            if (!isDir) {
              ByteStreams.copy(jarInput, jarOutput);
            }
          }

          jarEntry = jarInput.getNextJarEntry();
        }
      } finally {
        jarInput.close();
      }

      for (File embeddedJar : bundleEmbeddedJars) {
        JarEntry jarEntry = new JarEntry("lib/" + embeddedJar.getName());
        jarOutput.putNextEntry(jarEntry);
        Files.copy(embeddedJar, jarOutput);
      }

    } finally {
      jarOutput.close();
    }

    return new File(deployJar.toURI());
  }

  public static File createDeploymentJar(LocationFactory locationFactory, Class<?> clz, File...bundleEmbeddedJars)
    throws IOException {
    // Creates Manifest
    Manifest manifest = new Manifest();

    Attributes attributes = new Attributes();
    attributes.put(ManifestFields.MAIN_CLASS, clz.getName());
    attributes.put(ManifestFields.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().putAll(attributes);

    return createDeploymentJar(locationFactory, clz, manifest);
  }
 }
