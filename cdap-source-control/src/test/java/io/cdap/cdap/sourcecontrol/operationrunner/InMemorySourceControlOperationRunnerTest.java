/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.sourcecontrol.operationrunner;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.AuthConfig;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.PatConfig;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.sourcecontrol.AuthenticationConfigException;
import io.cdap.cdap.sourcecontrol.CommitMeta;
import io.cdap.cdap.sourcecontrol.GitOperationException;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.RepositoryManager;
import io.cdap.cdap.sourcecontrol.RepositoryManagerFactory;
import io.cdap.cdap.sourcecontrol.SourceControlAppConfigNotFoundException;
import io.cdap.cdap.sourcecontrol.SourceControlException;
import io.cdap.cdap.sourcecontrol.SourceControlTestBase;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class InMemorySourceControlOperationRunnerTest extends SourceControlTestBase {
  private static final String FAKE_COMMIT_HASH = "5905258bb958ceda80b6a37938050ad876920f10";
  private static final ApplicationDetail testAppDetails = new ApplicationDetail(
    TEST_APP_NAME, "v1", "description1", null, null, "conf1", new ArrayList<>(),
    new ArrayList<>(), new ArrayList<>(), null, null);
  private static final ApplicationDetail testApp2Details = new ApplicationDetail(
      TEST_APP2_NAME, "v1", "description1", null, null, "conf1", new ArrayList<>(),
      new ArrayList<>(), new ArrayList<>(), null, null);
  private static final RepositoryConfig testRepoConfig = new RepositoryConfig.Builder()
      .setProvider(Provider.GITHUB)
      .setLink("ignored")
      .setDefaultBranch("develop")
      .setPathPrefix(PATH_PREFIX)
      .setAuth(new AuthConfig(AuthType.PAT, new PatConfig("GITHUB_TOKEN_NAME", null)))
      .build();
  private static final CommitMeta testCommit =
    new CommitMeta("author1", "committer1", 123, "message1");
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final NamespaceId NAMESPACE = NamespaceId.DEFAULT;
  private static final PushAppOperationRequest pushContext =
    new PushAppOperationRequest(NAMESPACE, testRepoConfig, testAppDetails, testCommit);
  private static final MultiPushAppOperationRequest multiPushContext =
      new MultiPushAppOperationRequest(
          testRepoConfig,
          NAMESPACE,
          Collections.singletonList(testAppDetails),
          testCommit
      );
  private static final NamespaceRepository nameSpaceRepository = new NamespaceRepository(NAMESPACE, testRepoConfig);

  private InMemorySourceControlOperationRunner operationRunner;
  private RepositoryManager mockRepositoryManager;

  @Before
  public void setUp() throws Exception {
    RepositoryManagerFactory mockRepositoryManagerFactory = Mockito.mock(RepositoryManagerFactory.class);
    this.mockRepositoryManager = Mockito.mock(RepositoryManager.class);
    Mockito.doReturn(mockRepositoryManager).when(mockRepositoryManagerFactory).create(Mockito.any(), Mockito.any());
    Mockito.doReturn(FAKE_COMMIT_HASH).when(mockRepositoryManager).cloneRemote();
    Mockito.doReturn(
        Arrays.asList(FAKE_COMMIT_HASH, FAKE_COMMIT_HASH + "_2")
    ).when(mockRepositoryManager).commitAndPush(Mockito.any(), Mockito.anyList());
    this.operationRunner = new InMemorySourceControlOperationRunner(mockRepositoryManagerFactory);
    Path appRelativePath = Paths.get(PATH_PREFIX, testAppDetails.getName() + ".json");
    Mockito.doReturn(appRelativePath).when(mockRepositoryManager).getFileRelativePath(Mockito.any());
  }

  private boolean verifyConfigFileContent(Path repoDirPath) throws IOException {
    return verifyConfigFileContent(repoDirPath, testAppDetails);
  }

  private boolean verifyConfigFileContent(Path repoDirPath, ApplicationDetail app) throws IOException {
    String fileData = new String(Files.readAllBytes(
        repoDirPath.resolve(String.format("%s.json", app.getName()))), StandardCharsets.UTF_8);
    return fileData.equals(GSON.toJson(app));
  }

  @Test
  public void testPushSuccess() throws Exception {
    setupPushTest();
    Path tmpRepoDirPath = baseTempFolder.newFolder().toPath();
    Path baseRepoDirPath = tmpRepoDirPath.resolve(PATH_PREFIX);

    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getRepositoryRoot();
    Mockito.doReturn(baseRepoDirPath).when(mockRepositoryManager).getBasePath();
    Mockito.doReturn("file Hash").when(mockRepositoryManager).commitAndPush(Mockito.anyObject(),
                                                                            Mockito.any(Path.class));

    operationRunner.push(pushContext);

    Assert.assertTrue(verifyConfigFileContent(baseRepoDirPath));
  }

  @Test
  public void testMultiPushSuccess() throws Exception {
    setupPushTest();
    Path tmpRepoDirPath = baseTempFolder.newFolder().toPath();
    Path baseRepoDirPath = tmpRepoDirPath.resolve(PATH_PREFIX);

    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getRepositoryRoot();
    Mockito.doReturn(baseRepoDirPath).when(mockRepositoryManager).getBasePath();

    System.out.println(multiPushContext);
    operationRunner.push(multiPushContext, responses -> {
      Assert.assertTrue(responses.size() == 1);
    });

    Mockito.verify(mockRepositoryManager, Mockito.times(1)).cloneRemote();
    Mockito.verify(mockRepositoryManager, Mockito.times(1)).close();
    Assert.assertTrue(verifyConfigFileContent(baseRepoDirPath, testAppDetails));
  }

  @Test(expected = SourceControlException.class)
  public void testPushFailedToCreateDirectory() throws Exception {
    setupPushTest();
    // Setting the repo dir as file causing failure in Files.createDirectories
    Path tmpRepoDirPath = baseTempFolder.newFile().toPath();
    Path baseRepoDirPath = tmpRepoDirPath.resolve(PATH_PREFIX);

    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getRepositoryRoot();
    Mockito.doReturn(baseRepoDirPath).when(mockRepositoryManager).getBasePath();

    operationRunner.push(pushContext);
  }

  @Test(expected = SourceControlException.class)
  public void testPushFailedToWriteFile() throws Exception {
    setupPushTest();
    Path tmpRepoDirPath = baseTempFolder.newFolder().toPath();
    Path baseRepoDirPath = tmpRepoDirPath.resolve(PATH_PREFIX);
    // creating a directory where validateAppConfigRelativePath should throw SourceControlException
    Files.createDirectories(baseRepoDirPath.resolve(testAppDetails.getName() + ".json"));

    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getRepositoryRoot();
    Mockito.doReturn(baseRepoDirPath).when(mockRepositoryManager).getBasePath();

    operationRunner.push(pushContext);
  }

  @Test(expected = SourceControlException.class)
  public void testPushFailedInvalidSymlinkPath() throws Exception {
    setupPushTest();
    Path tmpRepoDirPath = baseTempFolder.newFolder().toPath();
    Path baseRepoDirPath = tmpRepoDirPath.resolve(PATH_PREFIX);

    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getRepositoryRoot();
    Mockito.doReturn(baseRepoDirPath).when(mockRepositoryManager).getBasePath();
    Mockito.doReturn("file Hash").when(mockRepositoryManager).commitAndPush(Mockito.anyObject(),
                                                                            Mockito.any(Path.class));

    Path target = tmpRepoDirPath.resolve("target");
    Files.createDirectories(baseRepoDirPath);
    Files.createFile(target);
    Files.createSymbolicLink(baseRepoDirPath.resolve("app1.json"), target);
    operationRunner.push(pushContext);
  }

  @Test(expected = AuthenticationConfigException.class)
  public void testPushFailedToClone() throws Exception {
    Mockito.doThrow(new AuthenticationConfigException("config not exists")).when(mockRepositoryManager).cloneRemote();
    operationRunner.push(pushContext);
  }

  @Test(expected = NoChangesToPushException.class)
  public void testPushNoChanges() throws Exception {
    setupPushTest();
    Path tmpRepoDirPath = baseTempFolder.newFolder().toPath();
    Path baseRepoDirPath = tmpRepoDirPath.resolve(PATH_PREFIX);

    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getRepositoryRoot();
    Mockito.doReturn(baseRepoDirPath).when(mockRepositoryManager).getBasePath();
    Mockito.doThrow(new NoChangesToPushException("no changes to push"))
      .when(mockRepositoryManager).commitAndPush(Mockito.any(), Mockito.anyList());
    operationRunner.push(pushContext);
  }

  @Test
  public void testListSuccess() throws Exception {
    Path tmpRepoDirPath = baseTempFolder.newFolder().toPath();

    Path file1 = tmpRepoDirPath.resolve("file1.json");
    Path file2 = tmpRepoDirPath.resolve("file2.some.json");
    Files.write(file1, new byte[]{});
    Files.write(file2, new byte[]{});

    // Skip non json file
    Files.write(tmpRepoDirPath.resolve("file3"), new byte[]{});
    // Skip directory
    Files.createDirectories(tmpRepoDirPath.resolve("file4.json"));
    // Skip symlink
    Files.createSymbolicLink(tmpRepoDirPath.resolve("file_link.json"), file1);

    RepositoryApp app1 = new RepositoryApp("file1", "testHash1");
    RepositoryApp app2 = new RepositoryApp("file2.some", "testHash2");

    Mockito.doReturn(FAKE_COMMIT_HASH).when(mockRepositoryManager).cloneRemote();
    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getBasePath();
    Mockito.doReturn(file1.getFileName()).when(mockRepositoryManager)
      .getFileRelativePath(file1.getFileName().toString());
    Mockito.doReturn(app1.getFileHash()).when(mockRepositoryManager)
      .getFileHash(file1.getFileName(), FAKE_COMMIT_HASH);
    Mockito.doReturn(file2.getFileName()).when(mockRepositoryManager)
      .getFileRelativePath(file2.getFileName().toString());
    Mockito.doReturn(app2.getFileHash()).when(mockRepositoryManager)
      .getFileHash(file2.getFileName(), FAKE_COMMIT_HASH);
    Mockito.doNothing().when(mockRepositoryManager).close();

    List<RepositoryApp> sortedListedApps =
      operationRunner.list(nameSpaceRepository).getApps().stream()
        .sorted(Comparator.comparing(RepositoryApp::getName)).collect(Collectors.toList());

    Assert.assertEquals(2, sortedListedApps.size());
    Assert.assertEquals(sortedListedApps.get(0), app1);
    Assert.assertEquals(sortedListedApps.get(1), app2);
  }

  @Test(expected = SourceControlException.class)
  public void testListFailedToClone() throws Exception {
    Mockito.doThrow(new IOException()).when(mockRepositoryManager).cloneRemote();
    Mockito.doNothing().when(mockRepositoryManager).close();
    operationRunner.list(nameSpaceRepository);
  }

  @Test(expected = NotFoundException.class)
  public void testListMissingBasePath() throws Exception {
    Mockito.doReturn(FAKE_COMMIT_HASH).when(mockRepositoryManager).cloneRemote();
    Mockito.doNothing().when(mockRepositoryManager).close();

    Path tmpRepoDirPath = baseTempFolder.newFolder().toPath();
    Mockito.doReturn(tmpRepoDirPath.resolve("missing")).when(mockRepositoryManager).getBasePath();

    operationRunner.list(nameSpaceRepository);
  }

  @Test(expected = GitOperationException.class)
  public void testListFailedToGetHash() throws Exception {
    Mockito.doReturn(FAKE_COMMIT_HASH).when(mockRepositoryManager).cloneRemote();
    Mockito.doNothing().when(mockRepositoryManager).close();

    Path tmpRepoDirPath = baseTempFolder.newFolder().toPath();
    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getBasePath();
    Path file1 = tmpRepoDirPath.resolve("file1.some.json");
    Files.write(file1, new byte[]{});
    Mockito.doReturn(file1.getFileName()).when(mockRepositoryManager).getFileRelativePath(Mockito.any());
    Mockito.doThrow(new IOException()).when(mockRepositoryManager).getFileHash(file1.getFileName(), FAKE_COMMIT_HASH);

    operationRunner.list(nameSpaceRepository);
  }

  @Test
  public void testPullSuccess() throws Exception {
    setupPullTest();
    ApplicationReference appRef = new ApplicationReference(NAMESPACE, TEST_APP_NAME);
    PullAppResponse<?> response = operationRunner.pull(new PullAppOperationRequest(appRef, testRepoConfig));
    validatePullResponse(response, TEST_APP_NAME);
    Mockito.verify(mockRepositoryManager, Mockito.times(1)).cloneRemote();
    Mockito.verify(mockRepositoryManager, Mockito.times(1)).close();
  }

  @Test
  public void testMultiPullSuccess() throws Exception {
    setupPullTest();
    List<PullAppResponse<?>> responses = new ArrayList<>();
    operationRunner.pull(
        new MultiPullAppOperationRequest(testRepoConfig, NAMESPACE, Sets.newHashSet(TEST_APP_NAME, TEST_APP2_NAME)),
        responses::add
    );
    Assert.assertEquals(responses.size(), 2);

    responses.sort(Comparator.comparing(PullAppResponse::getApplicationName));

    // validate
    validatePullResponse(responses.get(0), TEST_APP_NAME);
    validatePullResponse(responses.get(1), TEST_APP2_NAME);
    Mockito.verify(mockRepositoryManager, Mockito.times(1)).cloneRemote();
    Mockito.verify(mockRepositoryManager, Mockito.times(1)).close();
  }

  @Test(expected = NotFoundException.class)
  public void testPullFailedToReadHash() throws Exception {
    setupPullTest();
    ApplicationReference appRef = new ApplicationReference(NAMESPACE, TEST_APP_NAME);
    Mockito.doThrow(new NotFoundException("object not found"))
      .when(mockRepositoryManager)
      .getFileHash(Mockito.any(Path.class), Mockito.any(String.class));
    try {
      operationRunner.pull(new PullAppOperationRequest(appRef, testRepoConfig));
    } finally {
      Mockito.verify(mockRepositoryManager, Mockito.times(1)).cloneRemote();
      Mockito.verify(mockRepositoryManager, Mockito.times(1)).close();
    }
  }

  @Test(expected = SourceControlAppConfigNotFoundException.class)
  public void testPullFileNotFound() throws Exception {
    setupPullTest();
    ApplicationReference appRef = new ApplicationReference(NAMESPACE, TEST_APP_NAME);
    Mockito.doReturn(Paths.get(PATH_PREFIX, "notpresent.json"))
      .when(mockRepositoryManager).getFileRelativePath(Mockito.any());
    try {
      operationRunner.pull(new PullAppOperationRequest(appRef, testRepoConfig));
    } finally {
      Mockito.verify(mockRepositoryManager, Mockito.times(1)).cloneRemote();
      Mockito.verify(mockRepositoryManager, Mockito.times(1)).close();
    }
  }

  @Test(expected = SourceControlException.class)
  public void testPullCloneFailure() throws Exception {
    setupPullTest();
    ApplicationReference appRef = new ApplicationReference(NAMESPACE, TEST_APP_NAME);
    Mockito.doThrow(new IOException("secure store failure")).when(mockRepositoryManager).cloneRemote();
    try {
      operationRunner.pull(new PullAppOperationRequest(appRef, testRepoConfig));
    } finally {
      Mockito.verify(mockRepositoryManager, Mockito.times(1)).cloneRemote();
      Mockito.verify(mockRepositoryManager, Mockito.times(1)).close();
    }
  }

  private void setupPushTest() {
    Path appRelativePath = Paths.get(PATH_PREFIX, testAppDetails.getName() + ".json");
    Mockito.doReturn(appRelativePath).when(mockRepositoryManager).getFileRelativePath(Mockito.any());
  }

  private void setupPullTest() throws Exception {
    Path tmpRepoDirPath = baseTempFolder.newFolder().toPath();
    Path baseRepoDirPath = tmpRepoDirPath.resolve(PATH_PREFIX);
    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getRepositoryRoot();
    Mockito.doReturn(baseRepoDirPath).when(mockRepositoryManager).getBasePath();
    Mockito.doReturn(FAKE_COMMIT_HASH).when(mockRepositoryManager).cloneRemote();
    Mockito.doReturn(TEST_FILE_HASH)
      .when(mockRepositoryManager)
      .getFileHash(Mockito.eq(Paths.get(PATH_PREFIX, TEST_APP_NAME + ".json")), Mockito.eq(FAKE_COMMIT_HASH));
    Mockito.doReturn(TEST_FILE_HASH)
        .when(mockRepositoryManager)
        .getFileHash(Mockito.eq(Paths.get(PATH_PREFIX, TEST_APP2_NAME + ".json")), Mockito.eq(FAKE_COMMIT_HASH));
    Files.createDirectories(baseRepoDirPath);
    Files.write(baseRepoDirPath.resolve(TEST_APP_NAME + ".json"), TEST_APP_SPEC.getBytes(StandardCharsets.UTF_8));
    Files.write(baseRepoDirPath.resolve(TEST_APP2_NAME + ".json"), TEST_APP_SPEC.getBytes(StandardCharsets.UTF_8));
    Mockito.doNothing().when(mockRepositoryManager).close();
    Path appRelativePath = Paths.get(PATH_PREFIX, testAppDetails.getName() + ".json");
    Path app2RelativePath = Paths.get(PATH_PREFIX, testApp2Details.getName() + ".json");
    Mockito.doReturn(appRelativePath).when(mockRepositoryManager)
        .getFileRelativePath(testAppDetails.getName() + ".json");
    Mockito.doReturn(app2RelativePath).when(mockRepositoryManager)
        .getFileRelativePath(testApp2Details.getName() + ".json");
  }
}
