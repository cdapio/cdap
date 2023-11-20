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
import io.cdap.cdap.SupportBundleTestBase;
import io.cdap.cdap.SupportBundleTestHelper;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
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
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.twill.api.RunId;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Support bundle service tests.
 */
public class SupportBundleGeneratorTest extends SupportBundleTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleGeneratorTest.class);
  private static final Gson GSON = new GsonBuilder().create();
  private static final NamespaceId NAMESPACE = TEST_NAMESPACE_META1.getNamespaceId();

  private static SupportBundleGenerator supportBundleGenerator;
  private static CConfiguration cConf;
  private static Store store;
  private static ExecutorService executorService;

  private int sourceId;

  @BeforeClass
  public static void setup() throws Exception {
    Injector injector = getInjector();
    cConf = injector.getInstance(CConfiguration.class);
    supportBundleGenerator = injector.getInstance(SupportBundleGenerator.class);
    store = injector.getInstance(DefaultStore.class);

    executorService = Executors.newFixedThreadPool(cConf.getInt(Constants.SupportBundle.MAX_THREADS));
  }

  @AfterClass
  public static void shutdown() throws IOException {
    executorService.shutdownNow();
  }

  @Before
  public void setupNamespace() throws Exception {
    Assert.assertEquals(HttpURLConnection.HTTP_OK, createNamespace(NAMESPACE).getResponseCode());
  }

  @After
  public void cleanup() throws IOException {
    Assert.assertEquals(HttpURLConnection.HTTP_OK, deleteNamespace(NAMESPACE).getResponseCode());
  }

  @Test
  public void testSupportBundleService() throws Exception {
    deploy(AppWithWorkflow.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, NAMESPACE.getNamespace());
    long startTime = System.currentTimeMillis();

    HttpResponse appsResponse =
      doGet(getVersionedApiPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN,
                                NAMESPACE.getNamespace()));
    Assert.assertEquals(200, appsResponse.getResponseCode());
    String version = getResponseApplicationRecordVersion(appsResponse.getResponseBodyAsString());

    // We need the exact version here because setStartAndRunning() writes to store directly
    ProgramId workflowProgram = new ProgramId(NAMESPACE.getNamespace(), AppWithWorkflow.NAME, version,
                                              ProgramType.WORKFLOW, AppWithWorkflow.SampleWorkflow.NAME);

    RunId workflowRunId = RunIds.generate(startTime);
    ArtifactId artifactId = NAMESPACE.getNamespaceId().artifact("testArtifact", "1.0").toApiArtifactId();
    setStartAndRunning(workflowProgram, workflowRunId.getId(), artifactId);

    List<RunRecord> runs = getProgramRuns(workflowProgram, ProgramRunStatus.RUNNING);
    Assert.assertEquals(1, runs.size());

    // workflow ran for 1 minute
    long workflowStopTime = TimeUnit.MILLISECONDS.toSeconds(startTime) + 60;
    store.setStop(workflowProgram.run(workflowRunId.getId()), workflowStopTime, ProgramRunStatus.COMPLETED,
                  SupportBundleTestHelper.createSourceId(++sourceId));


    SupportBundleConfiguration bundleConfig =
      new SupportBundleConfiguration(NAMESPACE.getNamespace(), AppWithWorkflow.NAME, workflowRunId.getId(),
                                     ProgramType.valueOfCategoryName("workflows"), AppWithWorkflow.SampleWorkflow.NAME,
                                     1);

    String uuid = supportBundleGenerator.generate(bundleConfig, executorService);
    Assert.assertNotNull(uuid);
    File tempFolder = new File(cConf.get(Constants.SupportBundle.LOCAL_DATA_DIR));
    File uuidFile = new File(tempFolder, uuid);

    Tasks.waitFor(CollectionState.FINISHED, () -> {
      SupportBundleStatus supportBundleStatus = supportBundleGenerator.getBundleStatus(uuidFile);
      if (supportBundleStatus == null || supportBundleStatus.getStatus() == null) {
        return CollectionState.INVALID;
      }
      return supportBundleStatus.getStatus();
    }, 60, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
    SupportBundleStatus supportBundleStatus = supportBundleGenerator.getBundleStatus(uuidFile);

    Set<SupportBundleTaskStatus> supportBundleTaskStatusList = supportBundleStatus.getTasks();
    Assert.assertEquals(uuid, supportBundleStatus.getBundleId());
    Assert.assertEquals(CollectionState.FINISHED, supportBundleStatus.getStatus());

    for (SupportBundleTaskStatus supportBundleTaskStatus : supportBundleTaskStatusList) {
      Assert.assertEquals(CollectionState.FINISHED, supportBundleTaskStatus.getStatus());
    }
  }

  @Test
  public void testDeleteOldBundle() throws Exception {
    int maxFileNeedtoGenerate = 8;
    File tempFolder = new File(cConf.get(Constants.SupportBundle.LOCAL_DATA_DIR));
    String expectedDeletedUuid = "";
    for (int i = 0; i < maxFileNeedtoGenerate; i++) {
      String uuid = UUID.randomUUID().toString();
      File uuidFile = new File(tempFolder, uuid);
      DirUtils.mkdirs(uuidFile);
      if (i == 4) {
        expectedDeletedUuid = uuid;
        updateStatusToFail(tempFolder, uuid);
      } else {
        updateStatusToFinished(tempFolder, uuid);
      }
    }
    supportBundleGenerator.deleteOldFoldersIfExceedLimit(tempFolder);
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
    store.setProvisioning(id.run(pid), runtimeArgs, systemArgs, SupportBundleTestHelper.createSourceId(++sourceId),
                          artifactId);
    store.setProvisioned(id.run(pid), 0, SupportBundleTestHelper.createSourceId(++sourceId));
    store.setStart(id.run(pid), null, systemArgs, SupportBundleTestHelper.createSourceId(++sourceId));
    store.setRunning(id.run(pid), startTime + 1, null, SupportBundleTestHelper.createSourceId(++sourceId));
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

  private void updateStatusToFinished(File tempFolder, String uuid) {
    File bundleFile = new File(tempFolder, uuid);
    SupportBundleStatus supportBundleStatus = SupportBundleStatus.builder()
      .setBundleId(uuid)
      .setStatus(CollectionState.FINISHED)
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
