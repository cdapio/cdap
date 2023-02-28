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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.sourcecontrol.AuthenticationConfigException;
import io.cdap.cdap.sourcecontrol.CommitMeta;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.RepositoryManager;
import io.cdap.cdap.sourcecontrol.RepositoryManagerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

public class InMemorySourceControlOperationRunnerTest {
  private static final String FAKE_APP_NAME = "app1";
  private static final String FAKE_APP_SPEC = "{\n" +
      "  \"artifact\": {\n" +
      "     \"name\": \"cdap-notifiable-workflow\",\n" +
      "     \"version\": \"1.0.0\",\n" +
      "     \"scope\": \"system\"\n" +
      "  },\n" +
      "  \"config\": {\n" +
      "     \"plugin\": {\n" +
      "        \"name\": \"WordCount\",\n" +
      "        \"type\": \"sparkprogram\",\n" +
      "        \"artifact\": {\n" +
      "           \"name\": \"word-count-program\",\n" +
      "           \"scope\": \"user\",\n" +
      "           \"version\": \"1.0.0\"\n" +
      "        }\n" +
      "     }\n" +
      "  },\n" +
      "  \"preview\" : {\n" +
      "    \"programName\" : \"WordCount\",\n" +
      "    \"programType\" : \"spark\"\n" +
      "    },\n" +
      "  \"principal\" : \"test2\"\n" +
      "}";
  private static final String FAKE_FILE_HASH = "5905258bb958ceda80b6a37938050ad876920f09";
  private static final String FAKE_COMMIT_HASH = "5905258bb958ceda80b6a37938050ad876920f10";
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  private static final ApplicationDetail testAppDetails = new ApplicationDetail(
    "app1", "v1", "description1", null, null, "conf1", new ArrayList<>(),
    new ArrayList<>(), new ArrayList<>(), null, null);
  private static final String pathPrefix = "pathPrefix";
  private static final RepositoryConfig testRepoConfig = new RepositoryConfig.Builder()
    .setProvider(Provider.GITHUB)
    .setLink("ignored")
    .setDefaultBranch("develop")
    .setPathPrefix(pathPrefix)
    .setAuthType(AuthType.PAT)
    .setTokenName("GITHUB_TOKEN_NAME")
    .build();
  private static final CommitMeta testCommit = new CommitMeta("author1", "committer1", 123, "message1");
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final NamespaceId NAMESPACE = NamespaceId.DEFAULT;

  private InMemorySourceControlOperationRunner operationRunner;
  private RepositoryManager mockRepositoryManager;

  @Before
  public void setUp() throws Exception {
    RepositoryManagerFactory mockRepositoryManagerFactory = Mockito.mock(RepositoryManagerFactory.class);
    this.mockRepositoryManager = Mockito.mock(RepositoryManager.class);
    Mockito.doReturn(mockRepositoryManager).when(mockRepositoryManagerFactory).create(Mockito.any(), Mockito.any());
    Mockito.doReturn(FAKE_COMMIT_HASH).when(mockRepositoryManager).cloneRemote();
    this.operationRunner = new InMemorySourceControlOperationRunner(mockRepositoryManagerFactory);
    Path appRelativePath = Paths.get(pathPrefix, testAppDetails.getName() + ".json");
    Mockito.doReturn(appRelativePath).when(mockRepositoryManager).getFileRelativePath(Mockito.any());
  }

  private boolean verifyConfigFileContent(Path repoDirPath) throws IOException {
    String fileData = new String(Files.readAllBytes(
      repoDirPath.resolve(String.format("%s.json", testAppDetails.getName()))), StandardCharsets.UTF_8);
    return fileData.equals(GSON.toJson(testAppDetails));
  }

  @Test
  public void testPushSuccess() throws Exception {
    Path tmpRepoDirPath = TMP_FOLDER.newFolder().toPath();
    Path baseRepoDirPath = tmpRepoDirPath.resolve(pathPrefix);

    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getRepositoryRoot();
    Mockito.doReturn(baseRepoDirPath).when(mockRepositoryManager).getBasePath();
    Mockito.doReturn("file Hash").when(mockRepositoryManager).commitAndPush(Mockito.anyObject(),
                                                                            Mockito.any());

    operationRunner.push(NAMESPACE, testRepoConfig, testAppDetails, testCommit);

    Assert.assertTrue(verifyConfigFileContent(baseRepoDirPath));
  }

  @Test(expected = PushFailureException.class)
  public void testPushFailedToCreateDirectory() throws Exception {
    // Setting the repo dir as file causing failure in Files.createDirectories
    Path tmpRepoDirPath = TMP_FOLDER.newFile().toPath();
    Path baseRepoDirPath = tmpRepoDirPath.resolve(pathPrefix);

    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getRepositoryRoot();
    Mockito.doReturn(baseRepoDirPath).when(mockRepositoryManager).getBasePath();

    operationRunner.push(NAMESPACE, testRepoConfig, testAppDetails, testCommit);
  }

  @Test(expected = PushFailureException.class)
  public void testPushFailedToWriteFile() throws Exception {
    Path tmpRepoDirPath = TMP_FOLDER.newFolder().toPath();
    Path baseRepoDirPath = tmpRepoDirPath.resolve(pathPrefix);
    // creating a directory where validateAppConfigRelativePath should throw PushFailureException
    Files.createDirectories(baseRepoDirPath.resolve(testAppDetails.getName() + ".json"));

    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getRepositoryRoot();
    Mockito.doReturn(baseRepoDirPath).when(mockRepositoryManager).getBasePath();

    operationRunner.push(NAMESPACE, testRepoConfig, testAppDetails, testCommit);
  }

  @Test(expected = PushFailureException.class)
  public void testPushFailedInvalidSymlinkPath() throws Exception {
    Path tmpRepoDirPath = TMP_FOLDER.newFolder().toPath();
    Path baseRepoDirPath = tmpRepoDirPath.resolve(pathPrefix);

    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getRepositoryRoot();
    Mockito.doReturn(baseRepoDirPath).when(mockRepositoryManager).getBasePath();
    Mockito.doReturn("file Hash").when(mockRepositoryManager).commitAndPush(Mockito.anyObject(),
                                                                            Mockito.any());

    Path target = tmpRepoDirPath.resolve("target");
    Files.createDirectories(baseRepoDirPath);
    Files.createFile(target);
    Files.createSymbolicLink(baseRepoDirPath.resolve("app1.json"), target);
    operationRunner.push(NAMESPACE, testRepoConfig, testAppDetails, testCommit);
  }

  @Test(expected = AuthenticationConfigException.class)
  public void testPushFailedToClone() throws Exception {
    Mockito.doThrow(new AuthenticationConfigException("config not exists")).when(mockRepositoryManager).cloneRemote();
    operationRunner.push(NAMESPACE, testRepoConfig, testAppDetails, testCommit);
  }

  @Test(expected = NoChangesToPushException.class)
  public void testPushNoChanges() throws Exception {
    Path tmpRepoDirPath = TMP_FOLDER.newFolder().toPath();
    Path baseRepoDirPath = tmpRepoDirPath.resolve(pathPrefix);

    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getRepositoryRoot();
    Mockito.doReturn(baseRepoDirPath).when(mockRepositoryManager).getBasePath();
    Mockito.doThrow(new NoChangesToPushException("no changes to push"))
      .when(mockRepositoryManager).commitAndPush(Mockito.any(), Mockito.any());
    operationRunner.push(NAMESPACE, testRepoConfig, testAppDetails, testCommit);
  }

  @Test
  public void testPullSuccess() throws Exception {
    setupPullTest();
    ApplicationReference appRef = new ApplicationReference(NAMESPACE, FAKE_APP_NAME);
    PullAppResponse<?> response = operationRunner.pull(appRef, testRepoConfig);
    Assert.assertEquals(response.getApplicationFileHash(), FAKE_FILE_HASH);
    AppRequest<?> appRequest = response.getAppRequest();
    Assert.assertNotNull(appRequest.getArtifact());
    Assert.assertEquals("cdap-notifiable-workflow", appRequest.getArtifact().getName());
    Assert.assertNotNull(appRequest.getPreview());
    Assert.assertEquals("WordCount", appRequest.getPreview().getProgramName());
    Assert.assertEquals(response.getApplicationName(), FAKE_APP_NAME);
    Mockito.verify(mockRepositoryManager, Mockito.times(1)).cloneRemote();
    Mockito.verify(mockRepositoryManager, Mockito.times(1)).close();
  }

  @Test(expected = NotFoundException.class)
  public void testPullFailedToReadHash() throws Exception {
    setupPullTest();
    ApplicationReference appRef = new ApplicationReference(NAMESPACE, FAKE_APP_NAME);
    Mockito.doThrow(new NotFoundException("object not found"))
      .when(mockRepositoryManager)
      .getFileHash(Mockito.any(Path.class), Mockito.any(String.class));
    try {
      operationRunner.pull(appRef, testRepoConfig);
    } finally {
      Mockito.verify(mockRepositoryManager, Mockito.times(1)).cloneRemote();
      Mockito.verify(mockRepositoryManager, Mockito.times(1)).close();
    }
  }

  @Test(expected = NotFoundException.class)
  public void testPullFileNotFound() throws Exception {
    setupPullTest();
    ApplicationReference appRef = new ApplicationReference(NAMESPACE, FAKE_APP_NAME);
    Mockito.doReturn(Paths.get(pathPrefix, "app2.json")).when(mockRepositoryManager).getFileRelativePath(Mockito.any());
    try {
      operationRunner.pull(appRef, testRepoConfig);
    } finally {
      Mockito.verify(mockRepositoryManager, Mockito.times(1)).cloneRemote();
      Mockito.verify(mockRepositoryManager, Mockito.times(1)).close();
    }
  }

  @Test(expected = PullFailureException.class)
  public void testPullCloneFailure() throws Exception {
    setupPullTest();
    ApplicationReference appRef = new ApplicationReference(NAMESPACE, FAKE_APP_NAME);
    Mockito.doThrow(new IOException("secure store failure")).when(mockRepositoryManager).cloneRemote();
    try {
      operationRunner.pull(appRef, testRepoConfig);
    } finally {
      Mockito.verify(mockRepositoryManager, Mockito.times(1)).cloneRemote();
      Mockito.verify(mockRepositoryManager, Mockito.times(1)).close();
    }
  }

  private void setupPullTest() throws Exception {
    Path tmpRepoDirPath = TMP_FOLDER.newFolder().toPath();
    Path baseRepoDirPath = tmpRepoDirPath.resolve(pathPrefix);
    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getRepositoryRoot();
    Mockito.doReturn(baseRepoDirPath).when(mockRepositoryManager).getBasePath();
    Mockito.doReturn(FAKE_COMMIT_HASH).when(mockRepositoryManager).cloneRemote();
    Mockito.doReturn(FAKE_FILE_HASH)
      .when(mockRepositoryManager)
      .getFileHash(Mockito.eq(Paths.get(pathPrefix, FAKE_APP_NAME + ".json")), Mockito.eq(FAKE_COMMIT_HASH));
    Files.createDirectories(baseRepoDirPath);
    Files.write(baseRepoDirPath.resolve(FAKE_APP_NAME + ".json"), FAKE_APP_SPEC.getBytes(StandardCharsets.UTF_8));
    Mockito.doNothing().when(mockRepositoryManager).close();
  }
}
