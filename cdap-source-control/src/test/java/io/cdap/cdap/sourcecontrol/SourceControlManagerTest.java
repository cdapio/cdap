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
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryValidationFailure;
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
import java.util.UUID;

/**
 * Tests for {@link  SourceControlManager}.
 */
public class SourceControlManagerTest {
  private static final String DEFAULT_BRANCH_NAME = "develop";
  private static final String MOCK_TOKEN = UUID.randomUUID().toString();
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
    cConf.setInt(Constants.SourceControlManagement.GIT_COMMAND_TIMEOUT_SECONDS, 2);
    cConf.set(Constants.SourceControlManagement.GIT_REPOSITORIES_CLONE_DIRECTORY_PATH,
              tempFolder.newFolder("repository").getAbsolutePath());
  }

  @After
  public void afterEach() {
    testGit.close();
    bareGit.close();
  }

  @Test
  public void testValidate() throws Exception {
    // For GitHub + PAT, the username is the token and password is empty.
    LocalGitServer gitServer = new LocalGitServer(MOCK_TOKEN, "", 0, bareGit.getRepository());
    gitServer.start();

    String serverURL = gitServer.getServerURL();

    // Test Failure cases.
    // Provide incorrect token key name.
    RepositoryConfig config = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink(serverURL + "ignored")
      .setDefaultBranch("develop")
      .setAuthType(AuthType.PAT)
      .setTokenName(GITHUB_TOKEN_NAME + "invalid")
      .build();
    SourceControlContext context = new SourceControlContext(secureStore, config, new NamespaceId(NAMESPACE), cConf);
    SourceControlManager manager = new SourceControlManager(context);
    RepositoryValidationFailure failure = manager.validateConfig();
    Assert.assertNotNull(failure);
    Assert.assertTrue(failure.getMessage().contains("Failed to get password"));
    manager.close();

    // Provide invalid token from secure store.
    config = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink(serverURL + "ignored")
      .setDefaultBranch("develop")
      .setAuthType(AuthType.PAT)
      .setTokenName(GITHUB_TOKEN_NAME)
      .build();
    context = new SourceControlContext(secureStore, config, new NamespaceId(NAMESPACE), cConf);
    manager = new SourceControlManager(context);
    Mockito.when(secureStore.get(NAMESPACE, GITHUB_TOKEN_NAME))
      .thenReturn(new SecureStoreData(null, "invalid-token".getBytes(StandardCharsets.UTF_8)));
    failure = manager.validateConfig();
    Assert.assertNotNull(failure);
    Assert.assertTrue(failure.getMessage().contains("Authentication credentials invalid"));
    manager.close();

    // return correct token from secure store.
    Mockito.when(secureStore.get(NAMESPACE, GITHUB_TOKEN_NAME))
      .thenReturn(new SecureStoreData(null, MOCK_TOKEN.getBytes(StandardCharsets.UTF_8)));

    // Provide non-existent branch name.
    config = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink(serverURL + "ignored")
      .setDefaultBranch("develop-invalid")
      .setAuthType(AuthType.PAT)
      .setTokenName(GITHUB_TOKEN_NAME)
      .build();
    context = new SourceControlContext(secureStore, config, new NamespaceId(NAMESPACE), cConf);
    manager = new SourceControlManager(context);
    failure = manager.validateConfig();
    Assert.assertNotNull(failure);
    Assert.assertTrue(failure.getMessage().contains("Default branch not found in remote repository"));
    manager.close();

    // Provide null branch name.
    config = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink(serverURL + "ignored")
      .setAuthType(AuthType.PAT)
      .setTokenName(GITHUB_TOKEN_NAME)
      .build();
    context = new SourceControlContext(secureStore, config, new NamespaceId(NAMESPACE), cConf);
    manager = new SourceControlManager(context);
    failure = manager.validateConfig();
    Assert.assertNull(failure);
    manager.close();

    // Provide correct details
    config = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink(serverURL + "ignored")
      .setDefaultBranch("develop")
      .setAuthType(AuthType.PAT)
      .setTokenName(GITHUB_TOKEN_NAME)
      .build();
    context = new SourceControlContext(secureStore, config, new NamespaceId(NAMESPACE), cConf);
    manager = new SourceControlManager(context);
    failure = manager.validateConfig();
    Assert.assertNull(failure);
    manager.close();

    gitServer.stop();
  }
}
