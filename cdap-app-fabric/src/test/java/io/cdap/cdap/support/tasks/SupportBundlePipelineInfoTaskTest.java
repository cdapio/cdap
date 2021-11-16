/*
 * Copyright © 2021 Cask Data, Inc.
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
import com.google.gson.JsonObject;
import com.google.inject.Injector;
import io.cdap.cdap.AppWithWorkflow;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.logging.gateway.handlers.RemoteLogsFetcher;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramRunRecordFetcher;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramRunRecordsFetcher;
import io.cdap.cdap.metadata.RemoteApplicationDetailFetcher;
import io.cdap.cdap.metrics.process.RemoteMetricsSystemClient;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.support.job.SupportBundleJob;
import io.cdap.cdap.support.lib.SupportBundleFileNames;
import io.cdap.cdap.support.status.CollectionState;
import io.cdap.cdap.support.status.SupportBundleConfiguration;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.cdap.support.status.SupportBundleTaskStatus;
import io.cdap.cdap.support.task.SupportBundlePipelineInfoTask;
import io.cdap.cdap.support.task.factory.SupportBundlePipelineInfoTaskFactory;
import io.cdap.cdap.support.task.factory.SupportBundleSystemLogTaskFactory;
import io.cdap.cdap.support.task.factory.SupportBundleTaskFactory;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.api.RunId;
import org.iq80.leveldb.shaded.guava.util.concurrent.MoreExecutors;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class SupportBundlePipelineInfoTaskTest extends AppFabricTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(SupportBundlePipelineInfoTaskTest.class);
  private static final NamespaceId namespaceId = NamespaceId.DEFAULT;

  private static CConfiguration configuration;
  private static Store store;
  private static ExecutorService executorService;
  private static Set<SupportBundleTaskFactory> supportBundleTaskFactorySet;
  private static RemoteLogsFetcher remoteLogsFetcher;
  private static RemoteProgramRunRecordsFetcher remoteProgramRunRecordsFetcher;
  private static RemoteMetricsSystemClient remoteMetricsSystemClient;
  private static RemoteApplicationDetailFetcher remoteApplicationDetailFetcher;
  private static RemoteProgramRunRecordFetcher remoteProgramRunRecordFetcher;
  private static String workflowName;
  private static String application;
  private static ProgramType programType;
  private int sourceId;

  @BeforeClass
  public static void setup() throws Exception {
    Injector injector = getInjector();
    configuration = injector.getInstance(CConfiguration.class);
    store = injector.getInstance(DefaultStore.class);
    executorService = MoreExecutors.newDirectExecutorService();
    supportBundleTaskFactorySet = new HashSet<>();
    supportBundleTaskFactorySet.add(injector.getInstance(SupportBundlePipelineInfoTaskFactory.class));
    supportBundleTaskFactorySet.add(injector.getInstance(SupportBundleSystemLogTaskFactory.class));
    remoteLogsFetcher = injector.getInstance(RemoteLogsFetcher.class);
    remoteProgramRunRecordsFetcher = injector.getInstance(RemoteProgramRunRecordsFetcher.class);
    remoteMetricsSystemClient = injector.getInstance(RemoteMetricsSystemClient.class);
    remoteApplicationDetailFetcher = injector.getInstance(RemoteApplicationDetailFetcher.class);
    remoteProgramRunRecordFetcher = injector.getInstance(RemoteProgramRunRecordFetcher.class);
    workflowName = AppWithWorkflow.SampleWorkflow.NAME;
    application = AppWithWorkflow.NAME;
    programType = ProgramType.valueOfCategoryName("workflows");
  }

  //Contains two sub-task supportBundleRuntimeInfo and supportBundlePipelineRunLog
  //So we will test all three files together
  @Test
  public void testSupportBundlePipelineInfo() throws Exception {
    String runId = generateWorkflowLog();
    SupportBundleConfiguration supportBundleConfiguration =
      new SupportBundleConfiguration(namespaceId.getNamespace(), application, runId, programType, workflowName, 1);
    String uuid = UUID.randomUUID().toString();
    File tempFolder = new File(configuration.get(Constants.SupportBundle.LOCAL_DATA_DIR));
    File uuidFile = new File(tempFolder, uuid);
    SupportBundleStatus supportBundleStatus = SupportBundleStatus.builder()
      .setBundleId(uuid)
      .setStartTimestamp(System.currentTimeMillis())
      .setParameters(supportBundleConfiguration)
      .setStatus(CollectionState.IN_PROGRESS)
      .build();
    SupportBundleJob supportBundleJob =
      new SupportBundleJob(supportBundleTaskFactorySet, executorService, configuration, supportBundleStatus);
    SupportBundlePipelineInfoTask supportBundlePipelineInfoTask =
      new SupportBundlePipelineInfoTask(uuid, Collections.singletonList(namespaceId), application, null, uuidFile,
                                        remoteApplicationDetailFetcher, remoteProgramRunRecordsFetcher,
                                        remoteLogsFetcher, programType, workflowName, remoteMetricsSystemClient,
                                        supportBundleJob, 1, remoteProgramRunRecordFetcher);
    supportBundlePipelineInfoTask.collect();

    Set<SupportBundleTaskStatus> supportBundleTaskStatusList = supportBundleStatus.getTasks();

    for (SupportBundleTaskStatus supportBundleTaskStatus : supportBundleTaskStatusList) {
      if (!supportBundleTaskStatus.getName()
        .endsWith("SupportBundleSystemLogTask") && !supportBundleTaskStatus.getName()
        .endsWith("SupportBundlePipelineInfoTask")) {
        Assert.assertEquals(CollectionState.FINISHED, supportBundleTaskStatus.getStatus());
      }
    }

    File pipelineFolder = new File(uuidFile, AppWithWorkflow.NAME);
    File[] pipelineFiles =
      pipelineFolder.listFiles((dir, name) -> !name.startsWith(".") && !dir.isHidden() && dir.isDirectory());
    Assert.assertEquals(3, pipelineFiles.length);

    File pipelineInfoFile = new File(pipelineFolder, AppWithWorkflow.NAME + ".json");
    try (Reader reader = Files.newBufferedReader(pipelineInfoFile.toPath(), StandardCharsets.UTF_8)) {
      ApplicationDetail pipelineInfo = GSON.fromJson(reader, ApplicationDetail.class);
      Assert.assertEquals(AppWithWorkflow.NAME, pipelineInfo.getName());
      Assert.assertEquals("-SNAPSHOT", pipelineInfo.getAppVersion());
      Assert.assertEquals("Sample application", pipelineInfo.getDescription());
    } catch (Exception e) {
      LOG.error("Can not read pipelineInfo file ", e);
      Assert.fail();
    }

    File runInfoFile = new File(pipelineFolder, runId + ".json");
    try (Reader reader = Files.newBufferedReader(runInfoFile.toPath(), StandardCharsets.UTF_8)) {
      JsonObject runInfo = GSON.fromJson(reader, JsonObject.class);
      Assert.assertEquals("COMPLETED", runInfo.get("status").getAsString());
    } catch (Exception e) {
      LOG.error("Can not read status file ", e);
      Assert.fail();
    }

    File runLogFile = new File(pipelineFolder, runId + SupportBundleFileNames.LOG_SUFFIX_NAME);
    Assert.assertTrue(runLogFile.exists());
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

