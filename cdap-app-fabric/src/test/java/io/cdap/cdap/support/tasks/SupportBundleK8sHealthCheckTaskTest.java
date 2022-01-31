/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.support.tasks;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.Injector;
import io.cdap.cdap.AppWithWorkflow;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.metadata.RemoteHealthCheckFetcher;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.support.status.SupportBundleConfiguration;
import io.cdap.cdap.support.task.SupportBundleK8sHealthCheckTask;
import io.cdap.cdap.support.task.factory.SupportBundleK8sHealthCheckTaskFactory;
import io.cdap.cdap.support.task.factory.SupportBundlePipelineInfoTaskFactory;
import io.cdap.cdap.support.task.factory.SupportBundleSystemLogTaskFactory;
import io.cdap.cdap.support.task.factory.SupportBundleTaskFactory;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SupportBundleK8sHealthCheckTaskTest extends AppFabricTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleK8sHealthCheckTaskTest.class);
  private static final NamespaceId namespaceId = NamespaceId.DEFAULT;

  private static Store store;
  private static CConfiguration configuration;
  private static RemoteHealthCheckFetcher remoteHealthCheckFetcher;
  private static Set<SupportBundleTaskFactory> supportBundleTaskFactorySet;
  private static String workflowName;
  private static String application;
  private static ProgramType programType;
  private int sourceId;

  @BeforeClass
  public static void setup() throws Exception {
    Injector injector = getInjector();
    JsonArray podInfoList = new JsonArray();
    List<String> healthCheckServiceNameList = new ArrayList<>();
    healthCheckServiceNameList.add("health-check-appfabric-service");
    JsonObject pod1 = new JsonObject();
    pod1.addProperty("name", "appfabric");
    pod1.addProperty("status", "running");
    podInfoList.add(pod1);
    JsonObject pod2 = new JsonObject();
    pod2.addProperty("name", "supportbundle");
    pod2.addProperty("status", "failed");
    podInfoList.add(pod2);
    JsonArray nodeInfoList = new JsonArray();
    JsonObject node1 = new JsonObject();
    node1.addProperty("name", "appfabric");
    node1.addProperty("status", "running");
    nodeInfoList.add(node1);
    JsonArray eventInfoList = new JsonArray();
    JsonObject event1 = new JsonObject();
    event1.addProperty("name", "supportbundle");
    event1.addProperty("status", "running");
    eventInfoList.add(event1);

    configuration = injector.getInstance(CConfiguration.class);
    configuration.set(Constants.AppFabricHealthCheck.POD_INFO, podInfoList.toString());
    configuration.set(Constants.AppFabricHealthCheck.NODE_INFO, nodeInfoList.toString());
    configuration.set(Constants.AppFabricHealthCheck.EVENT_INFO, eventInfoList.toString());
    configuration.set(Constants.AppFabricHealthCheck.SERVICE_NAME_WITH_POD, Constants.AppFabricHealthCheck.POD_INFO);
    configuration.set(Constants.AppFabricHealthCheck.SERVICE_NAME_WITH_NODE, Constants.AppFabricHealthCheck.NODE_INFO);
    configuration.set(Constants.AppFabricHealthCheck.SERVICE_NAME_WITH_EVENT,
                      Constants.AppFabricHealthCheck.EVENT_INFO);
    configuration.setStrings(Constants.HealthCheck.SERVICE_NAME_LIST,
                             healthCheckServiceNameList.toArray(new String[healthCheckServiceNameList.size()]));

    store = injector.getInstance(DefaultStore.class);
    remoteHealthCheckFetcher = injector.getInstance(RemoteHealthCheckFetcher.class);

    supportBundleTaskFactorySet = new HashSet<>();
    supportBundleTaskFactorySet.add(injector.getInstance(SupportBundlePipelineInfoTaskFactory.class));
    supportBundleTaskFactorySet.add(injector.getInstance(SupportBundleSystemLogTaskFactory.class));
    supportBundleTaskFactorySet.add(injector.getInstance(SupportBundleK8sHealthCheckTaskFactory.class));
    workflowName = AppWithWorkflow.SampleWorkflow.NAME;
    application = AppWithWorkflow.NAME;
    programType = ProgramType.valueOfCategoryName("workflows");
  }

  @Test
  public void testSupportBundleK8sHealthCheckTask() throws Exception {
    String runId = generateWorkflowLog();
    SupportBundleConfiguration supportBundleConfiguration =
      new SupportBundleConfiguration(namespaceId.getNamespace(), application, runId, programType, workflowName, 1);
    String uuid = UUID.randomUUID().toString();
    File tempFolder = new File(configuration.get(Constants.SupportBundle.LOCAL_DATA_DIR));
    File uuidFile = new File(tempFolder, uuid);

    SupportBundleK8sHealthCheckTask supportBundleK8sHealthCheckTask =
      new SupportBundleK8sHealthCheckTask(configuration, uuidFile, remoteHealthCheckFetcher);
    supportBundleK8sHealthCheckTask.collect();

    File healthCheckFolder = new File(uuidFile, "health-check");
    File appFabricHealthCheckFolder = new File(healthCheckFolder, "health-check-appfabric-service");

    File healthCheckPodFile = new File(appFabricHealthCheckFolder, "supportbundle-pod-info.txt");
    try (Reader reader = Files.newBufferedReader(healthCheckPodFile.toPath(), StandardCharsets.UTF_8)) {
      JsonObject podInfo = GSON.fromJson(reader, JsonObject.class);
      Assert.assertEquals("failed", podInfo.get("status").getAsString());
    } catch (Exception e) {
      LOG.error("Can not read healthCheckPod file ", e);
      Assert.fail();
    }

    File healthCheckNodeFile = new File(appFabricHealthCheckFolder, "appfabric-node-info.txt");
    try (Reader reader = Files.newBufferedReader(healthCheckNodeFile.toPath(), StandardCharsets.UTF_8)) {
      JsonObject nodeInfo = GSON.fromJson(reader, JsonObject.class);
      Assert.assertEquals("running", nodeInfo.get("status").getAsString());
    } catch (Exception e) {
      LOG.error("Can not read healthCheckNode file ", e);
      Assert.fail();
    }

    File healthCheckEventFile = new File(appFabricHealthCheckFolder, "supportbundle-event-info.txt");
    try (Reader reader = Files.newBufferedReader(healthCheckEventFile.toPath(), StandardCharsets.UTF_8)) {
      JsonObject eventInfo = GSON.fromJson(reader, JsonObject.class);
      Assert.assertEquals("running", eventInfo.get("status").getAsString());
    } catch (Exception e) {
      LOG.error("Can not read healthCheckEvent file ", e);
      Assert.fail();
    }
  }

  private String generateWorkflowLog() throws Exception {
    deploy(AppWithWorkflow.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, namespaceId.getNamespace());
    long startTime = System.currentTimeMillis();

    ProgramId workflowProgram = new ProgramId(namespaceId.getNamespace(), AppWithWorkflow.NAME, ProgramType.WORKFLOW,
                                              AppWithWorkflow.SampleWorkflow.NAME);
    RunId workflowRunId = RunIds.generate(startTime);
    ArtifactId artifactId = namespaceId.artifact("testArtifact", "1.0").toApiArtifactId();
    setStartAndRunning(workflowProgram, workflowRunId.getId(), artifactId);

    List<RunRecord> runs = getProgramRuns(workflowProgram, ProgramRunStatus.RUNNING);
    Assert.assertEquals(1, runs.size());

    HttpResponse appsResponse =
      doGet(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, namespaceId.getNamespace()));
    Assert.assertEquals(200, appsResponse.getResponseCode());

    // workflow ran for 1 minute
    long workflowStopTime = TimeUnit.MILLISECONDS.toSeconds(startTime) + 60;
    store.setStop(workflowProgram.run(workflowRunId.getId()), workflowStopTime, ProgramRunStatus.COMPLETED,
                  AppFabricTestHelper.createSourceId(++sourceId));

    return runs.get(0).getPid();
  }

  private void setStartAndRunning(ProgramId id, String pid, ArtifactId artifactId) {
    setStartAndRunning(id, pid, ImmutableMap.of(), ImmutableMap.of(), artifactId);
  }

  private void setStartAndRunning(ProgramId id, String pid, Map<String, String> runtimeArgs,
                                  Map<String, String> systemArgs, ArtifactId artifactId) {
    if (!systemArgs.containsKey(SystemArguments.PROFILE_NAME)) {
      systemArgs = ImmutableMap.<String, String>builder()
        .putAll(systemArgs)
        .put(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName())
        .build();
    }
    long startTime = RunIds.getTime(pid, TimeUnit.SECONDS);
    store.setProvisioning(id.run(pid), runtimeArgs, systemArgs, AppFabricTestHelper.createSourceId(++sourceId),
                          artifactId);
    store.setProvisioned(id.run(pid), 0, AppFabricTestHelper.createSourceId(++sourceId));
    store.setStart(id.run(pid), null, systemArgs, AppFabricTestHelper.createSourceId(++sourceId));
    store.setRunning(id.run(pid), startTime + 1, null, AppFabricTestHelper.createSourceId(++sourceId));
  }
}
