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
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfigValidationException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Tests for {@link  RepositoryManager}.
 */
public class RepositoryManagerTest {
  private static final String DEFAULT_BRANCH_NAME = "develop";
  private static final String GIT_SERVER_USERNAME = "oauth2";
  private static final String MOCK_TOKEN = UUID.randomUUID().toString();
  private static final String NAMESPACE = "namespace1";
  private static final String GITHUB_TOKEN_NAME = "github-pat";
  @Mock
  private SecureStore secureStore;
  private CConfiguration cConf;
  public TemporaryFolder tempFolder = new TemporaryFolder();
  public LocalGitServer gitServer =
    new LocalGitServer(GIT_SERVER_USERNAME, MOCK_TOKEN, 0, DEFAULT_BRANCH_NAME, tempFolder);
  @Rule
  public RuleChain chain = RuleChain.outerRule(tempFolder).around(gitServer);

  @Before
  public void beforeEach() throws Exception {
    MockitoAnnotations.initMocks(this);
    Mockito.when(secureStore.get(NAMESPACE, GITHUB_TOKEN_NAME))
           .thenReturn(new SecureStoreData(null, MOCK_TOKEN.getBytes(StandardCharsets.UTF_8)));
    cConf = CConfiguration.create();
    cConf.setInt(Constants.SourceControlManagement.GIT_COMMAND_TIMEOUT_SECONDS, 2);
    cConf.set(Constants.SourceControlManagement.GIT_REPOSITORIES_CLONE_DIRECTORY_PATH,
              tempFolder.newFolder("repository").getAbsolutePath());
  }

  @Test
  public void testValidateIncorrectKeyName() {
    String serverURL = gitServer.getServerURL();
    RepositoryConfig config = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
                                                            .setLink(serverURL + "ignored")
                                                            .setDefaultBranch("develop")
                                                            .setAuthType(AuthType.PAT)
                                                            .setTokenName(GITHUB_TOKEN_NAME + "invalid")
                                                            .build();
    SourceControlConfig sourceControlConfig = new SourceControlConfig(new NamespaceId(NAMESPACE), config, cConf);
    boolean exceptionCaught = false;
    try {
      RepositoryManager.validateConfig(secureStore, sourceControlConfig);
    } catch (RepositoryConfigValidationException e) {
      exceptionCaught = true;
      Assert.assertTrue(e.getMessage().contains("Failed to get auth token"));
    }
    Assert.assertTrue(exceptionCaught);
  }

  @Test
  public void testValidateInvalidToken() throws Exception {
    String serverURL = gitServer.getServerURL();
    RepositoryConfig config = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
                                                            .setLink(serverURL + "ignored")
                                                            .setDefaultBranch("develop")
                                                            .setAuthType(AuthType.PAT)
                                                            .setTokenName(GITHUB_TOKEN_NAME)
                                                            .build();
    SourceControlConfig sourceControlConfig = new SourceControlConfig(new NamespaceId(NAMESPACE), config, cConf);
    Mockito.when(secureStore.get(NAMESPACE, GITHUB_TOKEN_NAME))
           .thenReturn(new SecureStoreData(null, "invalid-token".getBytes(StandardCharsets.UTF_8)));

    boolean exceptionCaught = false;
    try {
      RepositoryManager.validateConfig(secureStore, sourceControlConfig);
    } catch (RepositoryConfigValidationException e) {
      exceptionCaught = true;
      Assert.assertTrue(e.getMessage().contains("Authentication credentials invalid"));
    }
    Assert.assertTrue(exceptionCaught);
  }

  @Test
  public void testValidateInvalidBranchName() {
    String serverURL = gitServer.getServerURL();
    RepositoryConfig config = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
                                                            .setLink(serverURL + "ignored")
                                                            .setDefaultBranch("develop-invalid")
                                                            .setAuthType(AuthType.PAT)
                                                            .setTokenName(GITHUB_TOKEN_NAME)
                                                            .build();
    SourceControlConfig sourceControlConfig = new SourceControlConfig(new NamespaceId(NAMESPACE), config, cConf);
    boolean exceptionCaught = false;
    try {
      RepositoryManager.validateConfig(secureStore, sourceControlConfig);
    } catch (RepositoryConfigValidationException e) {
      exceptionCaught = true;
      Assert.assertTrue(e.getMessage().contains("Default branch not found in remote repository"));
    }
    Assert.assertTrue(exceptionCaught);
  }

  @Test
  public void testValidateNullBranchName() {
    String serverURL = gitServer.getServerURL();
    RepositoryConfig config = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
                                                            .setLink(serverURL + "ignored")
                                                            .setAuthType(AuthType.PAT)
                                                            .setTokenName(GITHUB_TOKEN_NAME)
                                                            .build();
    SourceControlConfig sourceControlConfig = new SourceControlConfig(new NamespaceId(NAMESPACE), config, cConf);
    RepositoryManager.validateConfig(secureStore, sourceControlConfig);
  }

  @Test
  public void testValidateSuccess() {
    String serverURL = gitServer.getServerURL();
    RepositoryConfig config = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
                                                            .setLink(serverURL + "ignored")
                                                            .setDefaultBranch("develop")
                                                            .setAuthType(AuthType.PAT)
                                                            .setTokenName(GITHUB_TOKEN_NAME)
                                                            .build();
    SourceControlConfig sourceControlConfig = new SourceControlConfig(new NamespaceId(NAMESPACE), config, cConf);
    RepositoryManager.validateConfig(secureStore, sourceControlConfig);
  }
}
