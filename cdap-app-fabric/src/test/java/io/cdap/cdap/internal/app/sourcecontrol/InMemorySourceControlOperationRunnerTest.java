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

package io.cdap.cdap.internal.app.sourcecontrol;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.sourcecontrol.CommitMeta;
import io.cdap.cdap.sourcecontrol.SourceControlManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

public class InMemorySourceControlOperationRunnerTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  private static final List<AppDetailsToPush> testAppDetails = Arrays.asList(
    new AppDetailsToPush("spec1", "app1", "author1"),
    new AppDetailsToPush("spec2", "app2", "author2")
  );
  private static final CommitMeta testCommit = new CommitMeta("author1", "commiter1", 123, "message1");
  ;

  private InMemorySourceControlOperationRunner operationRunner;
  private SourceControlManager mockSourceControlManager;

  @Before
  public void setUp() throws Exception {
    this.mockSourceControlManager = Mockito.mock(SourceControlManager.class);
    CConfiguration cConf = CConfiguration.create();
    this.operationRunner = new InMemorySourceControlOperationRunner(cConf, mockSourceControlManager);
  }

  private boolean verifyConfigFileContent(Path repoDirPath) throws IOException {
    for (AppDetailsToPush details : testAppDetails) {
      String fileData = new String(Files.readAllBytes(repoDirPath.resolve(details.getApplicationName())),
                                   StandardCharsets.UTF_8);
      if (!fileData.equals(details.getApplicationSpecString())) {
        return false;
      }
    }
    return true;
  }

  @Test
  public void testPushSuccess() throws Exception {
    Path tmpRepoDirPath = TMP_FOLDER.newFolder().toPath();

    Mockito.when(mockSourceControlManager.getBasePath()).thenReturn(tmpRepoDirPath);

    operationRunner.push(testAppDetails, testCommit, "testBranch");

    Mockito.verify(mockSourceControlManager, Mockito.times(1)).initialise();
    Mockito.verify(mockSourceControlManager, Mockito.times(1)).switchToCleanBranch("testBranch");
    Mockito.verify(mockSourceControlManager).push(testCommit);
    Assert.assertTrue(verifyConfigFileContent(tmpRepoDirPath));
  }

  @Test(expected = PushFailureException.class)
  public void testPushFailedToInitializeRepo() throws Exception {
    Path tmpRepoDirPath = TMP_FOLDER.newFolder().toPath();

    Mockito.when(mockSourceControlManager.getBasePath()).thenReturn(tmpRepoDirPath);
    Mockito.doThrow(new IOException()).when(mockSourceControlManager).initialise();

    operationRunner.push(testAppDetails, testCommit, "testBranch");
  }

  @Test(expected = PushFailureException.class)
  public void testPushFailedToSwitchBranch() throws Exception {
    Path tmpRepoDirPath = TMP_FOLDER.newFolder().toPath();
    Mockito.when(mockSourceControlManager.getBasePath()).thenReturn(tmpRepoDirPath);
    Mockito.doThrow(IOException.class).when(mockSourceControlManager).switchToCleanBranch("testBranch");

    operationRunner.push(testAppDetails, testCommit, "testBranch");
  }

  @Test(expected = PushFailureException.class)
  public void testPushFailedToCreateDirectory() throws Exception {
    // Setting the repo dir as file causing failure in Files.createDirectories
    File tmpRepoDir = TMP_FOLDER.newFile();

    Mockito.when(mockSourceControlManager.getBasePath()).thenReturn(tmpRepoDir.toPath());

    operationRunner.push(testAppDetails, testCommit, "testBranch");
  }

  @Test(expected = PushFailureException.class)
  public void testPushFailedToWriteFile() throws Exception {
    Path tmpRepoDirPath = TMP_FOLDER.newFolder().toPath();
    // creating a directory where config file should be causing failure in Files.write
    Files.createDirectories(tmpRepoDirPath.resolve(testAppDetails.get(1).getApplicationName()));

    Mockito.when(mockSourceControlManager.getBasePath()).thenReturn(tmpRepoDirPath);

    operationRunner.push(testAppDetails, testCommit, "testBranch");
  }

  @Test(expected = PushFailureException.class)
  public void testPushFailedToReadHash() throws Exception {
    Path tmpRepoDirPath = TMP_FOLDER.newFolder().toPath();

    Mockito.when(mockSourceControlManager.getBasePath()).thenReturn(tmpRepoDirPath);
    Mockito.when(mockSourceControlManager.getFileHash(Mockito.any(Path.class))).thenThrow(new IOException());

    operationRunner.push(testAppDetails, testCommit, "testBranch");
  }


}
