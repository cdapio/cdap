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
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotFoundException;
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
import io.cdap.cdap.support.lib.SupportBundleRequestFileList;
import io.cdap.cdap.support.status.CollectionState;
import io.cdap.cdap.support.status.SupportBundleConfiguration;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.cdap.support.status.SupportBundleTaskStatus;
import io.cdap.common.http.HttpResponse;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
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

  @Test
  public void testDownloadBundleFiles() throws Exception {
    File tempFolder = new File(cConf.get(Constants.SupportBundle.LOCAL_DATA_DIR));
    String uuid = UUID.randomUUID().toString();
    File uuidFile = new File(tempFolder, uuid);
    DirUtils.mkdirs(new File(uuidFile, "app"));
    updateStatusToFinished(tempFolder, uuid);
    Files.write(new File(uuidFile, "app/run.json").toPath(), "{}".getBytes(StandardCharsets.UTF_8));

    Path zipPath = Files.createTempFile(TEMP_FOLDER.newFolder().toPath(), "bundle", ".zip");
    supportBundleGenerator.createBundleZipByRequest(
        uuid, zipPath, new SupportBundleRequestFileList(
            Arrays.asList("app/run.json", "missing.json", "app/../app/run.json", "/status.json")));

    // Entry names are normalized and the same file is only added once.
    List<String> entryNames = new ArrayList<>();
    try (ZipInputStream zipIn = new ZipInputStream(Files.newInputStream(zipPath))) {
      for (ZipEntry entry = zipIn.getNextEntry(); entry != null; entry = zipIn.getNextEntry()) {
        entryNames.add(entry.getName());
      }
    }
    Assert.assertEquals(Arrays.asList(uuid + "/app/run.json", uuid + "/" + SupportBundleFileNames.STATUS_FILE_NAME),
                        entryNames);
  }

  @Test
  public void testDownloadRejectsFilesOutsideBundle() throws Exception {
    File tempFolder = new File(cConf.get(Constants.SupportBundle.LOCAL_DATA_DIR));
    String uuid = UUID.randomUUID().toString();
    DirUtils.mkdirs(new File(tempFolder, uuid));
    updateStatusToFinished(tempFolder, uuid);
    // A file next to the bundle directory, which must not be readable through the bundle API.
    File outsideFile = new File(tempFolder, "outside-" + uuid + ".txt");
    Files.write(outsideFile.toPath(), "secret".getBytes(StandardCharsets.UTF_8));
    // A symlink inside the bundle that points outside of it, where the filesystem supports it.
    boolean hasSymlink;
    try {
      Files.createSymbolicLink(new File(new File(tempFolder, uuid), "link").toPath(), outsideFile.toPath());
      hasSymlink = true;
    } catch (UnsupportedOperationException | IOException e) {
      hasSymlink = false;
    }

    Path zipPath = Files.createTempFile(TEMP_FOLDER.newFolder().toPath(), "bundle", ".zip");
    try {
      List<String> invalidPaths = new ArrayList<>(Arrays.asList(
          "../" + outsideFile.getName(), "app/../../" + outsideFile.getName(), "./../" + outsideFile.getName(),
          ".", "..", "", "status.json\0", null));
      if (hasSymlink) {
        invalidPaths.add("link");
      }
      for (String filePath : invalidPaths) {
        try {
          supportBundleGenerator.createBundleZipByRequest(
              uuid, zipPath, new SupportBundleRequestFileList(Collections.singletonList(filePath)));
          Assert.fail("Expected BadRequestException for file path " + filePath);
        } catch (BadRequestException e) {
          // expected
        }
      }
    } finally {
      Files.deleteIfExists(outsideFile.toPath());
    }
  }

  @Test
  public void testInvalidBundleIdIsNotFound() throws Exception {
    File tempFolder = new File(cConf.get(Constants.SupportBundle.LOCAL_DATA_DIR));
    String uuid = UUID.randomUUID().toString();
    DirUtils.mkdirs(new File(tempFolder, uuid));
    updateStatusToFinished(tempFolder, uuid);
    Path zipPath = Files.createTempFile(TEMP_FOLDER.newFolder().toPath(), "bundle", ".zip");

    for (String invalidId : Arrays.asList("..", "../" + uuid, uuid + "/..", "1-1-1-1-1", "")) {
      try {
        supportBundleGenerator.getBundle(invalidId);
        Assert.fail("Expected NotFoundException for get of " + invalidId);
      } catch (NotFoundException e) {
        // expected
      }
      try {
        supportBundleGenerator.deleteBundle(invalidId);
        Assert.fail("Expected NotFoundException for delete of " + invalidId);
      } catch (NotFoundException e) {
        // expected
      }
      try {
        supportBundleGenerator.createBundleZipByRequest(
            invalidId, zipPath, new SupportBundleRequestFileList(Collections.emptyList()));
        Assert.fail("Expected NotFoundException for download of " + invalidId);
      } catch (NotFoundException e) {
        // expected
      }
    }
    // The valid bundle is untouched.
    Assert.assertNotNull(supportBundleGenerator.getBundle(uuid));
    Assert.assertTrue(new File(tempFolder, uuid).isDirectory());
  }

  @Test
  public void testGenerateRejectsInvalidIdentifiers() throws Exception {
    String validRun = RunIds.generate().getId();
    List<SupportBundleConfiguration> invalidConfigs = Arrays.asList(
        new SupportBundleConfiguration(NAMESPACE.getNamespace(), AppWithWorkflow.NAME,
                                       "../../../../../../../preferences", ProgramType.WORKFLOW,
                                       AppWithWorkflow.SampleWorkflow.NAME, 1),
        new SupportBundleConfiguration(NAMESPACE.getNamespace(), "../app", validRun, ProgramType.WORKFLOW,
                                       AppWithWorkflow.SampleWorkflow.NAME, 1),
        new SupportBundleConfiguration(NAMESPACE.getNamespace(), AppWithWorkflow.NAME, validRun,
                                       ProgramType.WORKFLOW, "../workflow", 1),
        new SupportBundleConfiguration("../ns", AppWithWorkflow.NAME, validRun, ProgramType.WORKFLOW,
                                       AppWithWorkflow.SampleWorkflow.NAME, 1));
    for (SupportBundleConfiguration config : invalidConfigs) {
      try {
        supportBundleGenerator.generate(config, executorService);
        Assert.fail("Expected BadRequestException for " + GSON.toJson(config));
      } catch (BadRequestException e) {
        // expected
      }
    }
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
