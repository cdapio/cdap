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

package io.cdap.cdap.support.services;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Injector;
import io.cdap.cdap.AppWithWorkflow;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.SupportBundle;
import io.cdap.cdap.common.utils.Tasks;
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
import io.cdap.cdap.support.lib.SupportBundleFileNames;
import io.cdap.cdap.support.status.CollectionState;
import io.cdap.cdap.support.status.SupportBundleConfiguration;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.cdap.support.status.SupportBundleTaskStatus;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Support bundle service tests.
 */
public class SupportBundleServiceTest extends AppFabricTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleService.class);
  private static final Gson GSON = new GsonBuilder().create();
  private static final NamespaceId namespaceId = NamespaceId.DEFAULT;

  private static SupportBundleService supportBundleService;
  private static CConfiguration configuration;
  private static Store store;
  private int sourceId;

  @BeforeClass
  public static void setup() {
    Injector injector = getInjector();
    configuration = injector.getInstance(CConfiguration.class);
    supportBundleService = injector.getInstance(SupportBundleService.class);
    store = injector.getInstance(DefaultStore.class);
  }

  @Test
  public void testSupportBundleService() throws Exception {
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


    SupportBundleConfiguration supportBundleConfiguration =
      new SupportBundleConfiguration(namespaceId.getNamespace(), AppWithWorkflow.NAME, workflowRunId.getId(),
                                     ProgramType.valueOfCategoryName("workflows"), AppWithWorkflow.SampleWorkflow.NAME,
                                     1);

    String uuid = supportBundleService.generateSupportBundle(supportBundleConfiguration);
    Assert.assertNotNull(uuid);
    File tempFolder = new File(configuration.get(SupportBundle.LOCAL_DATA_DIR));
    File uuidFile = new File(tempFolder, uuid);

    Tasks.waitFor(CollectionState.FINISHED, () -> {
      SupportBundleStatus supportBundleStatus = supportBundleService.getSingleBundleJson(uuidFile);
      if (supportBundleStatus == null) {
        return CollectionState.INVALID;
      }
      return supportBundleService.getSingleBundleJson(uuidFile).getStatus();
    }, 60, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
    SupportBundleStatus supportBundleStatus = supportBundleService.getSingleBundleJson(uuidFile);

    Set<SupportBundleTaskStatus> supportBundleTaskStatusList = supportBundleStatus.getTasks();
    Assert.assertEquals(uuid, supportBundleStatus.getBundleId());
    Assert.assertEquals(CollectionState.FINISHED, supportBundleStatus.getStatus());

    for (SupportBundleTaskStatus supportBundleTaskStatus : supportBundleTaskStatusList) {
      Assert.assertEquals(CollectionState.FINISHED, supportBundleTaskStatus.getStatus());
    }
  }

  @Test
  public void testDeleteOldBundle() throws Exception {
    File tempFolder = new File(configuration.get(Constants.SupportBundle.LOCAL_DATA_DIR));
    createNamespace("default");
    String path = String.format("%s/support/bundle?namespace=default", Constants.Gateway.API_VERSION_3);
    String expectedDeletedUuid = "";
    for (int i = 0; i < 8; i++) {
      HttpResponse response = doPost(path);
      Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
      String uuid = response.getResponseBodyAsString();
      File uuidFile = new File(tempFolder, uuid);
      File statusFile = new File(uuidFile, SupportBundleFileNames.STATUS_FILE_NAME);
      Tasks.waitFor(true, statusFile::exists, 60, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
      if (i == 4) {
        expectedDeletedUuid = uuid;
        updateStatusToFail(tempFolder, uuid);
      }
    }
    //Exceed the maximum number of folder allows in bundle
    File expectedDeletedBundle = new File(tempFolder.getPath(), expectedDeletedUuid);
    Assert.assertFalse(expectedDeletedBundle.exists());
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

  private void updateStatusToFail(File tempFolder, String uuid) {
    File bundleFile = new File(tempFolder, uuid);
    SupportBundleStatus supportBundleStatus = SupportBundleStatus.builder()
      .setBundleId(uuid)
      .setStatus(CollectionState.FAILED)
      .setParameters(null)
      .setStartTimestamp(System.currentTimeMillis())
      .build();
    try (FileWriter statusFile = new FileWriter(new File(bundleFile, SupportBundleFileNames.STATUS_FILE_NAME))) {
      GSON.toJson(supportBundleStatus, statusFile);
    } catch (Exception e) {
      LOG.error("Can not update status file ", e);
      Assert.fail();
    }
  }
}
