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

package io.cdap.cdap.sourcecontrol;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.sourcecontrol.operationrunner.InMemorySourceControlOperationRunner;
import io.cdap.cdap.sourcecontrol.operationrunner.PushFailureException;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class InMemorySourceControlOperationRunnerTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  private static final List<ApplicationDetail> testAppDetails = Arrays.asList(
    new ApplicationDetail("app1", "v1", "description1", null, null, "conf1", new ArrayList<>(),
                          new ArrayList<>(), new ArrayList<>(), null, null),
    new ApplicationDetail("app2", "v1", "description2", null, null, "conf2", new ArrayList<>(),
                          new ArrayList<>(), new ArrayList<>(), null, null)
  );
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
  }

  private boolean verifyConfigFileContent(Path repoDirPath) throws IOException {
    for (ApplicationDetail details : testAppDetails) {
      String fileData = new String(Files.readAllBytes(repoDirPath.resolve(String.format("%s.json", details.getName()))),
                                   StandardCharsets.UTF_8);
      if (!fileData.equals(GSON.toJson(details))) {
        return false;
      }
    }
    return true;
  }

  @Test
  public void testPushSuccess() throws Exception {
    Path tmpRepoDirPath = TMP_FOLDER.newFolder().toPath();

    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getBasePath();
    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getRepositoryRoot();
    Mockito.doReturn("file Hash").when(mockRepositoryManager).getFileHash(Mockito.any(Path.class),
                                                                             Mockito.any(String.class));
    Mockito.doReturn("file Hash").when(mockRepositoryManager).commitAndPush(Mockito.anyObject(),
                                                                            Mockito.anyList());

    Path target = tmpRepoDirPath.resolve("target");
    Files.createFile(target);
    Files.createLink(tmpRepoDirPath.resolve("app1.json"), target);
    operationRunner.push(NAMESPACE, testAppDetails, testCommit);

    Assert.assertTrue(verifyConfigFileContent(tmpRepoDirPath));
  }

  @Test(expected = PushFailureException.class)
  public void testPushFailedToCreateDirectory() throws Exception {
    // Setting the repo dir as file causing failure in Files.createDirectories
    Path tmpRepoDirPath = TMP_FOLDER.newFile().toPath();

    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getBasePath();

    operationRunner.push(NAMESPACE, testAppDetails, testCommit);
  }

  @Test(expected = PushFailureException.class)
  public void testPushFailedToWriteFile() throws Exception {
    Path tmpRepoDirPath = TMP_FOLDER.newFolder().toPath();
    // creating a directory where config file should be causing failure in Files.write
    Files.createDirectories(tmpRepoDirPath.resolve(testAppDetails.get(1).getName()));

    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getBasePath();

    operationRunner.push(NAMESPACE, testAppDetails, testCommit);
  }

  @Test(expected = PushFailureException.class)
  public void testPushFailedInvalidPath() throws Exception {
    Path tmpRepoDirPath = TMP_FOLDER.newFolder().toPath();
    Path target = tmpRepoDirPath.getParent().resolve("target");
    Files.createFile(target);
    Files.createLink(tmpRepoDirPath.resolve("app1.json"), target);

    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getBasePath();

    operationRunner.push(NAMESPACE, testAppDetails, testCommit);
  }

  @Test(expected = PushFailureException.class)
  public void testPushFailedToCommit() throws Exception {
    Path tmpRepoDirPath = TMP_FOLDER.newFolder().toPath();

    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getBasePath();
    Mockito.doThrow(new UnexpectedRepositoryChangesException("")).when(mockRepositoryManager)
      .commitAndPush(Mockito.anyObject(), Mockito.anyList());

    operationRunner.push(NAMESPACE, testAppDetails, testCommit);
  }

  @Test(expected = PushFailureException.class)
  public void testPushFailedToReadHash() throws Exception {
    Path tmpRepoDirPath = TMP_FOLDER.newFolder().toPath();

    Mockito.doReturn(tmpRepoDirPath).when(mockRepositoryManager).getBasePath();
    Mockito.doThrow(new IOException()).when(mockRepositoryManager).getFileHash(Mockito.any(Path.class),
                                                                               Mockito.any(String.class));

    operationRunner.push(NAMESPACE, testAppDetails, testCommit);
  }
}
