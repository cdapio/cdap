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

package io.cdap.cdap.support.job;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.cdap.cdap.AppWithWorkflow;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
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
import io.cdap.cdap.support.SupportBundleTaskConfiguration;
import io.cdap.cdap.support.status.CollectionState;
import io.cdap.cdap.support.status.SupportBundleConfiguration;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.cdap.support.status.SupportBundleTaskStatus;
import io.cdap.cdap.support.task.factory.SupportBundlePipelineInfoTaskFactory;
import io.cdap.cdap.support.task.factory.SupportBundleSystemLogTaskFactory;
import io.cdap.cdap.support.task.factory.SupportBundleTaskFactory;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.api.RunId;
import org.iq80.leveldb.shaded.guava.util.concurrent.MoreExecutors;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
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
public class SupportBundleJobTest extends AppFabricTestBase {
  private static final NamespaceId namespaceId = NamespaceId.DEFAULT;

  private static CConfiguration configuration;
  private static Store store;
  private static ExecutorService executorService;
  private static Set<SupportBundleTaskFactory> supportBundleTaskFactorySet;
  private static String workflowName;
  private static String application;
  private static ProgramType programType;
  private static String runId;
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
    long startTime = System.currentTimeMillis();

    workflowName = AppWithWorkflow.SampleWorkflow.NAME;
    application = AppWithWorkflow.NAME;
    programType = ProgramType.valueOfCategoryName("workflows");
    RunId workflowRunId = RunIds.generate(startTime);
    runId = workflowRunId.getId();
  }

  @Test
  public void testSupportBundleJobExecute() throws Exception {
    generateWorkflowLog();
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
    DirUtils.mkdirs(uuidFile);
    SupportBundleJob supportBundleJob =
      new SupportBundleJob(supportBundleTaskFactorySet, executorService, configuration, supportBundleStatus);

    SupportBundleTaskConfiguration taskConfiguration =
      new SupportBundleTaskConfiguration(supportBundleConfiguration, uuid, uuidFile,
                                         Collections.singletonList(namespaceId), supportBundleJob);
    supportBundleJob.generateBundle(taskConfiguration);


    Set<SupportBundleTaskStatus> supportBundleTaskStatusList = supportBundleStatus.getTasks();
    Assert.assertEquals(uuid, supportBundleStatus.getBundleId());

    for (SupportBundleTaskStatus supportBundleTaskStatus : supportBundleTaskStatusList) {
      Assert.assertEquals(CollectionState.FINISHED, supportBundleTaskStatus.getStatus());
    }
  }

  private void generateWorkflowLog() throws Exception {
    deploy(AppWithWorkflow.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, namespaceId.getNamespace());
    long startTime = System.currentTimeMillis();

    ProgramId workflowProgram = new ProgramId(namespaceId.getNamespace(), AppWithWorkflow.NAME, ProgramType.WORKFLOW,
                                              AppWithWorkflow.SampleWorkflow.NAME);

    ArtifactId artifactId = namespaceId.artifact("testArtifact", "1.0").toApiArtifactId();
    setStartAndRunning(workflowProgram, runId, artifactId);

    List<RunRecord> runs = getProgramRuns(workflowProgram, ProgramRunStatus.RUNNING);
    Assert.assertEquals(1, runs.size());

    HttpResponse appsResponse =
      doGet(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, namespaceId.getNamespace()));
    Assert.assertEquals(200, appsResponse.getResponseCode());

    // workflow ran for 1 minute
    long workflowStopTime = TimeUnit.MILLISECONDS.toSeconds(startTime) + 60;
    store.setStop(workflowProgram.run(runId), workflowStopTime, ProgramRunStatus.COMPLETED,
                  AppFabricTestHelper.createSourceId(++sourceId));

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
