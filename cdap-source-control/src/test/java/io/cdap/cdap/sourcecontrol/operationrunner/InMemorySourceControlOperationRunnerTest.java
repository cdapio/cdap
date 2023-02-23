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
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.NamespaceId;
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
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  private static final ApplicationDetail testAppDetails = new ApplicationDetail(
    "app1", "v1", "description1", null, null, "conf1", new ArrayList<>(),
    new ArrayList<>(), new ArrayList<>(), null, null);
  private static final String pathPrefix = "pathPrefix";
  private static final CommitMeta testCommit = new CommitMeta("author1", "committer1", 123, "message1");
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final NamespaceId NAMESPACE = NamespaceId.DEFAULT;
  private InMemorySourceControlOperationRunner operationRunner;
  private RepositoryManager mockRepositoryManager;

  @Before
  public void setUp() throws Exception {
    RepositoryManagerFactory mockRepositoryManagerFactory = Mockito.mock(RepositoryManagerFactory.class);
    this.mockRepositoryManager = Mockito.mock(RepositoryManager.class);
    Mockito.doReturn(mockRepositoryManager).when(mockRepositoryManagerFactory).create(Mockito.any());
    Mockito.doReturn("commit hash").when(mockRepositoryManager).cloneRemote();
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

    operationRunner.push(NAMESPACE, testAppDetails, testCommit);

    Assert.assertTrue(verifyConfigFileContent(baseRepoDirPath));
  }

  @Test(expected = PushFailureException.class)
  public void testPushFailedToCreateDirectory() throws Exception {
    // Setting the repo dir as file causing failure in Files.createDirectories
    Path tmpRepoDirPath = TMP_FOLDER.newFile().toPath();
    Path baseRepoDirPath = tmpRepoDirPath.resolve(pathPrefix);

    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getRepositoryRoot();
    Mockito.doReturn(baseRepoDirPath).when(mockRepositoryManager).getBasePath();

    operationRunner.push(NAMESPACE, testAppDetails, testCommit);
  }

  @Test(expected = PushFailureException.class)
  public void testPushFailedToWriteFile() throws Exception {
    Path tmpRepoDirPath = TMP_FOLDER.newFolder().toPath();
    Path baseRepoDirPath = tmpRepoDirPath.resolve(pathPrefix);
    // creating a directory where validateAppConfigRelativePath should throw PushFailureException
    Files.createDirectories(baseRepoDirPath.resolve(testAppDetails.getName() + ".json"));

    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getRepositoryRoot();
    Mockito.doReturn(baseRepoDirPath).when(mockRepositoryManager).getBasePath();

    operationRunner.push(NAMESPACE, testAppDetails, testCommit);
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
    operationRunner.push(NAMESPACE, testAppDetails, testCommit);
  }

  @Test(expected = AuthenticationConfigException.class)
  public void testPushFailedToClone() throws Exception {
    Mockito.doThrow(new AuthenticationConfigException("config not exists")).when(mockRepositoryManager).cloneRemote();
    operationRunner.push(NAMESPACE, testAppDetails, testCommit);
  }

  @Test(expected = NoChangesToPushException.class)
  public void testPushNoChanges() throws Exception {
    Path tmpRepoDirPath = TMP_FOLDER.newFolder().toPath();
    Path baseRepoDirPath = tmpRepoDirPath.resolve(pathPrefix);

    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getRepositoryRoot();
    Mockito.doReturn(baseRepoDirPath).when(mockRepositoryManager).getBasePath();
    Mockito.doThrow(new NoChangesToPushException("no changes to push"))
      .when(mockRepositoryManager).commitAndPush(Mockito.any(), Mockito.any());
    operationRunner.push(NAMESPACE, testAppDetails, testCommit);
  }
}
