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

import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.AuthConfig;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import org.eclipse.jgit.api.Git;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

public class SourceControlManagerTest {
  private static final String DEFAULT_BRANCH_NAME = "develop";
  private static final String MOCK_TOKEN = "abc123456";
  private static final String NAMESPACE = "namespace1";
  private static final String GITHUB_TOKEN_NAME = "github-pat";
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  // Bare git repo to serve as a remote for testing.
  private Git bareGit;
  // Git used to push/pull from bare git repo.
  private Git testGit;
  @Mock
  private SecureStore secureStore;
  private CConfiguration cConf;

  @Before
  public void beforeEach() throws Exception {
    MockitoAnnotations.initMocks(this);
    Mockito.when(secureStore.get(NAMESPACE, GITHUB_TOKEN_NAME))
      .thenReturn(new SecureStoreData(null, MOCK_TOKEN.getBytes(StandardCharsets.UTF_8)));

    // Initialise git repositories.
    File bareRepoDir = tempFolder.newFolder("bare_repo");
    bareGit = Git.init().setDirectory(bareRepoDir).setBare(true).call();
    File testRepoDir = tempFolder.newFolder("temp_test_repo");
    testGit = Git.init().setDirectory(testRepoDir).call();

    // We need to initialise history by committing before switching branches.
    Files.write(testGit.getRepository().getDirectory().toPath().resolve("abc.txt"), "content".getBytes());
    testGit.add().addFilepattern(".").call();
    testGit.commit().setMessage("main_message").call();
    testGit.checkout().setCreateBranch(true).setName(DEFAULT_BRANCH_NAME).call();

    testGit.push().setPushAll().setRemote(bareRepoDir.toPath().toString()).call();
    cConf = CConfiguration.create();
    cConf.setInt(Constants.SourceControl.GIT_COMMAND_TIMEOUT_SECONDS, 2);
    cConf.set(Constants.SourceControl.GIT_REPOSITORIES_CLONE_DIRECTORY_PATH,
              tempFolder.newFolder("repository").getAbsolutePath());
  }

  @After
  public void afterEach() {
    testGit.close();
    bareGit.close();
  }

  @Test
  public void testValidate() throws Exception {
    File localRepoDir = tempFolder.newFolder("local_repo");

    // For GitHub + PAT, the username is the token and password is empty.
    LocalGitServer gitServer = new LocalGitServer(MOCK_TOKEN, "", 0, bareGit.getRepository());
    gitServer.start();

    String serverURL = gitServer.getServerURL();

    // Test Failure cases.
    // Provide incorrect token key name.
    AuthConfig authConfig = new AuthConfig(AuthType.PAT, GITHUB_TOKEN_NAME + "invalid", null);
    RepositoryConfig config = new RepositoryConfig(Provider.GITHUB, serverURL + "ignored", "develop", authConfig, "");
    SourceControlContext context =
      new SourceControlContext(secureStore, config, new NamespaceId(NAMESPACE), cConf);
    SourceControlManager manager = new SourceControlManager(context);
    List<Exception> errors = manager.validateConfig();
    Assert.assertEquals(1, errors.size());
    Assert.assertTrue(errors.get(0).getMessage().contains("Failed to get password"));
    manager.close();

    // Provide invalid token from secure store.
    authConfig = new AuthConfig(AuthType.PAT, GITHUB_TOKEN_NAME, null);
    config = new RepositoryConfig(Provider.GITHUB, serverURL + "ignored", "develop", authConfig, "");
    context = new SourceControlContext(secureStore, config, new NamespaceId(NAMESPACE), cConf);
    manager = new SourceControlManager(context);
    Mockito.when(secureStore.get(NAMESPACE, GITHUB_TOKEN_NAME))
      .thenReturn(new SecureStoreData(null, "invalid-token".getBytes(StandardCharsets.UTF_8)));
    errors = manager.validateConfig();
    Assert.assertEquals(1, errors.size());
    Assert.assertTrue(errors.get(0).getMessage().contains("Authentication credentials may be invalid"));
    manager.close();

    // return correct token from secure store.
    Mockito.when(secureStore.get(NAMESPACE, GITHUB_TOKEN_NAME))
      .thenReturn(new SecureStoreData(null, MOCK_TOKEN.getBytes(StandardCharsets.UTF_8)));

    // Provide non-existent branch name.
    authConfig = new AuthConfig(AuthType.PAT, GITHUB_TOKEN_NAME, null);
    config = new RepositoryConfig(Provider.GITHUB, serverURL + "ignored", "invalid-branch", authConfig, "");
    context = new SourceControlContext(secureStore,  config, new NamespaceId(NAMESPACE), cConf);
    manager = new SourceControlManager(context);
    errors = manager.validateConfig();
    Assert.assertEquals(1, errors.size());
    Assert.assertTrue(errors.get(0) instanceof IllegalArgumentException);
    Assert.assertTrue(errors.get(0).getMessage().contains("Default branch not found in remote repository"));
    manager.close();

    // Provide correct details
    authConfig = new AuthConfig(AuthType.PAT, GITHUB_TOKEN_NAME, null);
    config = new RepositoryConfig(Provider.GITHUB, serverURL + "ignored", "develop", authConfig, "");
    context = new SourceControlContext(secureStore, config, new NamespaceId(NAMESPACE), cConf);
    manager = new SourceControlManager(context);
    errors = manager.validateConfig();
    Assert.assertEquals(0, errors.size());
    manager.close();

    gitServer.stop();
  }
}
