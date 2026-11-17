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

import com.google.common.base.Strings;
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

  private final List<File> createdBundles = new ArrayList<>();
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
    for (File bundle : createdBundles) {
      DirUtils.deleteDirectoryContents(bundle, false);
    }
    createdBundles.clear();
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
    File uuidFile = createFinishedBundle();
    String uuid = uuidFile.getName();
    DirUtils.mkdirs(new File(uuidFile, "app"));
    Files.write(new File(uuidFile, "app/run.json").toPath(), "{}".getBytes(StandardCharsets.UTF_8));

    Path zipPath = createZipPath();
    supportBundleGenerator.createBundleZipByRequest(
        uuid, zipPath, new SupportBundleRequestFileList(
            Arrays.asList("app/run.json", "missing.json", "app/../app/run.json", "/status.json")));

    // Entry names are normalized, missing files are skipped and the same file is only added once.
    Assert.assertEquals(Arrays.asList(uuid + "/app/run.json", uuid + "/" + SupportBundleFileNames.STATUS_FILE_NAME),
                        listZipEntries(zipPath));
  }

  @Test
  public void testDownloadWholeBundleSkipsSymlinks() throws Exception {
    File uuidFile = createFinishedBundle();
    String uuid = uuidFile.getName();
    DirUtils.mkdirs(new File(uuidFile, "app"));
    Files.write(new File(uuidFile, "app/run.json").toPath(), "{}".getBytes(StandardCharsets.UTF_8));
    File outsideFile = createOutsideFile(uuidFile);
    try {
      boolean hasSymlink = createSymlink(new File(uuidFile, "link"), outsideFile);

      Path zipPath = createZipPath();
      supportBundleGenerator.createBundleZipByRequest(
          uuid, zipPath, new SupportBundleRequestFileList(Collections.emptyList()));

      List<String> entries = listZipEntries(zipPath);
      Assert.assertTrue(entries.contains(uuid + "/"));
      Assert.assertTrue(entries.contains(uuid + "/app/"));
      Assert.assertTrue(entries.contains(uuid + "/app/run.json"));
      Assert.assertTrue(entries.contains(uuid + "/" + SupportBundleFileNames.STATUS_FILE_NAME));
      if (hasSymlink) {
        Assert.assertFalse(entries.contains(uuid + "/link"));
      }
    } finally {
      Files.deleteIfExists(outsideFile.toPath());
    }
  }

  @Test
  public void testDownloadRejectsInvalidFilePaths() throws Exception {
    File uuidFile = createFinishedBundle();
    String uuid = uuidFile.getName();
    DirUtils.mkdirs(new File(uuidFile, "app"));
    File outsideFile = createOutsideFile(uuidFile);
    try {
      List<String> invalidPaths = new ArrayList<>(Arrays.asList(
          "../" + outsideFile.getName(), "app/../../" + outsideFile.getName(), "./../" + outsideFile.getName(),
          ".", "..", "", "app", "app/", "status.json\0", null,
          // Longer than the filesystem path limit, so it cannot be canonicalized.
          String.join("/", Collections.nCopies(25, Strings.repeat("b", 200)))));
      if (createSymlink(new File(uuidFile, "link"), outsideFile)) {
        invalidPaths.add("link");
      }

      Path zipPath = createZipPath();
      for (String filePath : invalidPaths) {
        assertBadRequest("file path " + filePath, () -> supportBundleGenerator.createBundleZipByRequest(
            uuid, zipPath, new SupportBundleRequestFileList(Collections.singletonList(filePath))));
      }
    } finally {
      Files.deleteIfExists(outsideFile.toPath());
    }
  }

  @Test
  public void testDownloadRejectsMissingFileList() throws Exception {
    String uuid = createFinishedBundle().getName();
    Path zipPath = createZipPath();
    assertBadRequest("null request", () -> supportBundleGenerator.createBundleZipByRequest(uuid, zipPath, null));
    assertBadRequest("null file list", () -> supportBundleGenerator.createBundleZipByRequest(
        uuid, zipPath, new SupportBundleRequestFileList(null)));
  }

  @Test
  public void testInvalidBundleIdIsNotFound() throws Exception {
    String uuid = createFinishedBundle().getName();
    Path zipPath = createZipPath();

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
  }

  @Test
  public void testGenerateRejectsInvalidParameters() throws Exception {
    String namespace = NAMESPACE.getNamespace();
    String app = AppWithWorkflow.NAME;
    String program = AppWithWorkflow.SampleWorkflow.NAME;
    String validRun = RunIds.generate().getId();
    List<SupportBundleConfiguration> invalidConfigs = Arrays.asList(
        new SupportBundleConfiguration(namespace, app, "../../../../../../../preferences", ProgramType.WORKFLOW,
                                       program, 1),
        new SupportBundleConfiguration(namespace, app, "1-1-1-1-1", ProgramType.WORKFLOW, program, 1),
        new SupportBundleConfiguration(namespace, app, "", ProgramType.WORKFLOW, program, 1),
        new SupportBundleConfiguration(namespace, "../app", validRun, ProgramType.WORKFLOW, program, 1),
        new SupportBundleConfiguration(namespace, app, validRun, ProgramType.WORKFLOW, "../workflow", 1),
        new SupportBundleConfiguration(namespace, app, validRun, ProgramType.WORKFLOW, null, 1),
        new SupportBundleConfiguration(namespace, app, validRun, null, program, 1),
        new SupportBundleConfiguration(namespace, app, validRun, ProgramType.WORKFLOW, program, 0),
        new SupportBundleConfiguration(namespace, app, validRun, ProgramType.WORKFLOW, program, -1),
        new SupportBundleConfiguration("../ns", app, validRun, ProgramType.WORKFLOW, program, 1));
    for (SupportBundleConfiguration config : invalidConfigs) {
      assertBadRequest(GSON.toJson(config), () -> supportBundleGenerator.generate(config, executorService));
    }
  }

  /**
   * Creates a bundle directory with a finished status, which is deleted after the test.
   */
  private File createFinishedBundle() throws IOException {
    File tempFolder = new File(cConf.get(Constants.SupportBundle.LOCAL_DATA_DIR));
    String uuid = UUID.randomUUID().toString();
    File uuidFile = new File(tempFolder, uuid);
    DirUtils.mkdirs(uuidFile);
    createdBundles.add(uuidFile);
    updateStatusToFinished(tempFolder, uuid);
    return uuidFile;
  }

  /**
   * Creates a file next to the given bundle directory, which must not be readable through the bundle API.
   */
  private static File createOutsideFile(File uuidFile) throws IOException {
    File outsideFile = new File(uuidFile.getParentFile(), "outside-" + uuidFile.getName() + ".txt");
    Files.write(outsideFile.toPath(), "secret".getBytes(StandardCharsets.UTF_8));
    return outsideFile;
  }

  /**
   * Creates a symbolic link, returning {@code false} if the filesystem does not support it.
   */
  private static boolean createSymlink(File link, File target) {
    try {
      Files.createSymbolicLink(link.toPath(), target.toPath());
      return true;
    } catch (UnsupportedOperationException | IOException e) {
      return false;
    }
  }

  private static Path createZipPath() throws IOException {
    return Files.createTempFile(TEMP_FOLDER.newFolder().toPath(), "bundle", ".zip");
  }

  private static List<String> listZipEntries(Path zipPath) throws IOException {
    List<String> entryNames = new ArrayList<>();
    try (ZipInputStream zipIn = new ZipInputStream(Files.newInputStream(zipPath))) {
      for (ZipEntry entry = zipIn.getNextEntry(); entry != null; entry = zipIn.getNextEntry()) {
        entryNames.add(entry.getName());
      }
    }
    return entryNames;
  }

  private static void assertBadRequest(String description, ThrowingRunnable runnable) throws Exception {
    try {
      runnable.run();
      Assert.fail("Expected BadRequestException for " + description);
    } catch (BadRequestException e) {
      // expected
    }
  }

  /**
   * A runnable that can throw exceptions.
   */
  private interface ThrowingRunnable {
    void run() throws Exception;
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
