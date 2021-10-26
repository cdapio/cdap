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
import io.cdap.cdap.WorkflowApp;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.support.services.SupportBundleService;
import io.cdap.cdap.support.status.CollectionState;
import io.cdap.cdap.support.status.SupportBundleConfiguration;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.cdap.support.status.SupportBundleTaskStatus;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class SupportBundleJobTest extends AppFabricTestBase {
  private static final ApplicationId WORKFLOW_APP = NamespaceId.DEFAULT.app("WorkflowApp");
  private static final String RUNNING = "RUNNING";
  private static SupportBundleService supportBundleService;
  private static CConfiguration configuration;
  private static Store store;
  private int sourceId;

  @BeforeClass
  public static void setup() throws Exception {
    configuration = CConfiguration.create();
    Injector injector = AppFabricTestHelper.getInjector();
    store = injector.getInstance(DefaultStore.class);
    supportBundleService = injector.getInstance(SupportBundleService.class);
    configuration.set(Constants.SupportBundle.LOCAL_DATA_DIR,
                      tmpFolder.newFolder().getAbsolutePath());
  }

  @Test
  public void testSupportBundleService() throws Exception {
    deploy(WorkflowApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN,
           WORKFLOW_APP.getNamespace());
    long startTime = System.currentTimeMillis();

    ProgramId workflowProgram =
    new ProgramId(
    WORKFLOW_APP.getNamespace(),
    WORKFLOW_APP.getApplication(),
    ProgramType.WORKFLOW,
    WorkflowApp.FunWorkflow.NAME);
    RunId workflowRunId = RunIds.generate(startTime);
    ArtifactId artifactId =
    WORKFLOW_APP.getNamespaceId().artifact("testArtifact", "1.0").toApiArtifactId();
    setStartAndRunning(workflowProgram, workflowRunId.getId(), artifactId);

    // start a program
    startProgram(workflowProgram);
    waitState(workflowProgram, RUNNING);

    List<RunRecord> runs = getProgramRuns(workflowProgram, ProgramRunStatus.RUNNING);
    Assert.assertEquals(1, runs.size());

    // stop the program
    stopProgram(workflowProgram);

    HttpResponse appsResponse =
    doGet(
    getVersionedAPIPath(
    "apps/",
    Constants.Gateway.API_VERSION_3_TOKEN,
    WORKFLOW_APP.getNamespaceId().getNamespace()));
    Assert.assertEquals(200, appsResponse.getResponseCode());

    SupportBundleConfiguration supportBundleConfiguration = new SupportBundleConfiguration(null,
                                                                                           null,
                                                                                           null,
                                                                                           null, 1);
    String uuid = supportBundleService.generateSupportBundle(supportBundleConfiguration);
    Assert.assertNotNull(uuid);
    String tmpFolderPath =  configuration.get(Constants.SupportBundle.LOCAL_DATA_DIR);
    File uuidFile = new File(tmpFolderPath, uuid);
    SupportBundleStatus supportBundleStatus = supportBundleService.getSingleBundleJson(uuidFile);
    List<SupportBundleTaskStatus> supportBundleTaskStatusList = supportBundleStatus.getTasks();
    Assert.assertEquals(uuid, supportBundleStatus.getBundleId());
    Assert.assertEquals(CollectionState.FINISHED, supportBundleStatus.getStatus());
    Assert.assertEquals(uuid, supportBundleStatus.getTasks());

    for (SupportBundleTaskStatus supportBundleTaskStatus : supportBundleTaskStatusList) {
      Assert.assertEquals(CollectionState.FINISHED, supportBundleTaskStatus.getStatus());
    }

    tmpFolder.delete();
  }

  private void setStartAndRunning(ProgramId id,
                                  String pid,
                                  ArtifactId artifactId) {
    setStartAndRunning(id, pid, ImmutableMap.of(), ImmutableMap.of(), artifactId);
  }

  private void setStartAndRunning(ProgramId id,
                                  String pid,
                                  Map<String, String> runtimeArgs,
                                  Map<String, String> systemArgs,
                                  ArtifactId artifactId) {
    if (!systemArgs.containsKey(SystemArguments.PROFILE_NAME)) {
      systemArgs =
      ImmutableMap.<String, String>builder()
      .putAll(systemArgs)
      .put(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName())
      .build();
    }
    long startTime = RunIds.getTime(pid, TimeUnit.SECONDS);
    store.setProvisioning(id.run(pid), runtimeArgs, systemArgs,
                          AppFabricTestHelper.createSourceId(++sourceId), artifactId);
    store.setProvisioned(id.run(pid), 0, AppFabricTestHelper.createSourceId(++sourceId));
    store.setStart(id.run(pid), null, systemArgs, AppFabricTestHelper.createSourceId(++sourceId));
    store.setRunning(
    id.run(pid), startTime + 1, null, AppFabricTestHelper.createSourceId(++sourceId));
  }
}
