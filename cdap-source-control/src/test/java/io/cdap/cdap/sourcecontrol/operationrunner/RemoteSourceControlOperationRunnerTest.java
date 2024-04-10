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

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.HttpExceptionHandler;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.internal.remote.DefaultInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.internal.remote.TaskWorkerHttpHandlerInternal;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.InMemoryNamespaceAdmin;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.AuthConfig;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.PatConfig;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.store.FileSecureStoreService;
import io.cdap.cdap.security.store.SecureStoreHandler;
import io.cdap.cdap.security.store.client.RemoteSecureStore;
import io.cdap.cdap.sourcecontrol.AuthenticationConfigException;
import io.cdap.cdap.sourcecontrol.CommitMeta;
import io.cdap.cdap.sourcecontrol.LocalGitServer;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.SecureSystemReader;
import io.cdap.cdap.sourcecontrol.SourceControlAppConfigNotFoundException;
import io.cdap.cdap.sourcecontrol.SourceControlException;
import io.cdap.cdap.sourcecontrol.SourceControlTestBase;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentDecompressor;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.eclipse.jgit.util.SystemReader;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link RemoteSourceControlOperationRunner}
 */
public class RemoteSourceControlOperationRunnerTest extends SourceControlTestBase {
  private static final ApplicationReference mockAppRef = new ApplicationReference(NAMESPACE, TEST_APP_NAME);
  private static final ApplicationDetail mockAppDetails = new ApplicationDetail(
    TEST_APP_NAME, "v1", "description1", null, null, "conf1", new ArrayList<>(),
    new ArrayList<>(), new ArrayList<>(), null, null);
  private static final CommitMeta mockCommit = new CommitMeta("author", "commiter", System.currentTimeMillis(),
                                                              "commit");
  private static final AuthConfig AUTH_CONFIG = new AuthConfig(AuthType.PAT,
      new PatConfig(PASSWORD_NAME, GIT_SERVER_USERNAME));
  private static final AuthConfig NON_EXISTS_AUTH_CONFIG = new AuthConfig(
      AuthType.PAT, new PatConfig(PASSWORD_NAME + "not_exist", null));
  @ClassRule
  public static final TemporaryFolder TEST_TEMP_FOLDER = new TemporaryFolder();
  private static final RepositoryConfig mockRepoConfig =
      new RepositoryConfig.Builder()
          .setProvider(Provider.GITHUB)
          .setLink("ignored")
          .setDefaultBranch(DEFAULT_BRANCH_NAME)
          .setPathPrefix(PATH_PREFIX)
          .setAuth(AUTH_CONFIG)
          .build();
  private static CConfiguration cConf;
  private static SConfiguration sConf;
  private static NettyHttpService httpService;
  private static RemoteClientFactory remoteClientFactory;
  private static MetricsCollectionService metricsCollectionService;
  private static RemoteSecureStore remoteSecureStore;
  public LocalGitServer gitServer = getGitServer();
  @Rule
  public RuleChain chain = RuleChain.outerRule(baseTempFolder).around(gitServer);

  @BeforeClass
  public static void init() throws Exception {
    // Set up CConf, SConf and necessary services
    cConf = CConfiguration.create();
    cConf.setInt(Constants.TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 0);
    cConf.setInt(Constants.SourceControlManagement.GIT_COMMAND_TIMEOUT_SECONDS, GIT_COMMAND_TIMEOUT);
    cConf.set(Constants.SourceControlManagement.GIT_REPOSITORIES_CLONE_DIRECTORY_PATH,
        TEST_TEMP_FOLDER.newFolder("repository").getAbsolutePath());

    metricsCollectionService = new NoOpMetricsCollectionService();
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    remoteClientFactory = new RemoteClientFactory(discoveryService,
                                                  new DefaultInternalAuthenticator(new AuthenticationTestContext()));

    sConf = SConfiguration.create();
    cConf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, true);
    cConf.set(Constants.Security.Store.FILE_PATH, TEST_TEMP_FOLDER.newFolder("secureStore").getAbsolutePath());
    sConf.set(Constants.Security.Store.FILE_PASSWORD, "secret");
    cConf.setInt(Constants.ArtifactLocalizer.PORT, -1);
    InMemoryNamespaceAdmin namespaceClient = new InMemoryNamespaceAdmin();

    NamespaceMeta namespaceMeta = new NamespaceMeta.Builder().setName(NAMESPACE).build();
    namespaceClient.create(namespaceMeta);

    FileSecureStoreService fileSecureStoreService = new FileSecureStoreService(cConf, sConf, namespaceClient,
                                                                               FileSecureStoreService.CURRENT_CODEC
                                                                                 .newInstance());

    httpService = new CommonNettyHttpServiceBuilder(cConf, "test", new NoOpMetricsCollectionService(), null)
      .setHttpHandlers(
        new TaskWorkerHttpHandlerInternal(cConf, discoveryService, discoveryService, className -> {
        }, new NoOpMetricsCollectionService()),
        new SecureStoreHandler(fileSecureStoreService, fileSecureStoreService)
      )
      .setChannelPipelineModifier(new ChannelPipelineModifier() {
        @Override
        public void modify(ChannelPipeline pipeline) {
          pipeline.addAfter("compressor", "decompressor", new HttpContentDecompressor());
        }
      })
      .setExceptionHandler(new HttpExceptionHandler())
      .build();

    // Start the http service that runs TaskWorkerHttpHandler and register services
    httpService.start();
    discoveryService.register(URIScheme.createDiscoverable(Constants.Service.TASK_WORKER, httpService));
    discoveryService.register(URIScheme.createDiscoverable(Constants.Service.SECURE_STORE_SERVICE, httpService));

    // Set up the git token
    remoteSecureStore = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        bind(RemoteClientFactory.class).toInstance(remoteClientFactory);
      }
    }).getInstance(RemoteSecureStore.class);
    remoteSecureStore.put(NAMESPACE,
        PASSWORD_NAME, MOCK_TOKEN, "fake git token", ImmutableMap.of("prop1", "value1"));
  }

  @AfterClass
  public static void finish() throws Exception {
    httpService.stop();
  }

  @Test
  public void testPushSuccess() throws Exception {
    // Get the actual repo config
    String serverUrl = gitServer.getServerUrl();
    RepositoryConfig repoConfig =
        new RepositoryConfig.Builder(mockRepoConfig).setLink(serverUrl + "ignored").build();
    PushAppOperationRequest mockPushContext =
        new PushAppOperationRequest(new NamespaceId(NAMESPACE), repoConfig, mockAppDetails, mockCommit);

    RemoteSourceControlOperationRunner operationRunner =
      new RemoteSourceControlOperationRunner(cConf, metricsCollectionService, remoteClientFactory);

    PushAppsResponse pushResponse = operationRunner.push(mockPushContext);
    // Verify SecureSystemReader is being used.
    Assert.assertTrue(SystemReader.getInstance() instanceof SecureSystemReader);

    PushAppMeta meta = pushResponse.getApps().iterator().next();

    // Assert the pushed app in response
    Assert.assertEquals(meta.getName(), mockAppDetails.getName());
    Assert.assertEquals(meta.getVersion(), mockAppDetails.getAppVersion());
  }

  @Test(expected = SourceControlException.class)
  public void testPushFailureException() throws Exception {
    // Get the actual repo config
    String serverUrl = gitServer.getServerUrl();
    RepositoryConfig repoConfig =
        new RepositoryConfig.Builder(mockRepoConfig).setLink(serverUrl + "ignored").build();
    PushAppOperationRequest mockPushContext =
        new PushAppOperationRequest(new NamespaceId(NAMESPACE), repoConfig, mockAppDetails, mockCommit);

    RemoteSourceControlOperationRunner operationRunner =
      new RemoteSourceControlOperationRunner(cConf, metricsCollectionService, remoteClientFactory);

    // close the remote repository
    gitServer.after();
    operationRunner.push(mockPushContext);
  }

  @Test(expected = NoChangesToPushException.class)
  public void testNoChangesToPushException() throws Exception {
    // Get the actual repo config
    String serverUrl = gitServer.getServerUrl();
    RepositoryConfig repoConfig =
        new RepositoryConfig.Builder(mockRepoConfig).setLink(serverUrl + "ignored").build();
    PushAppOperationRequest mockPushContext =
        new PushAppOperationRequest(new NamespaceId(NAMESPACE), repoConfig, mockAppDetails, mockCommit);

    RemoteSourceControlOperationRunner operationRunner =
      new RemoteSourceControlOperationRunner(cConf, metricsCollectionService, remoteClientFactory);

    operationRunner.push(mockPushContext);
    // Push the same unchanged application again
    operationRunner.push(mockPushContext);
  }

  @Test(expected = AuthenticationConfigException.class)
  public void testPushAuthenticationConfigException() throws Exception {
    String serverUrl = gitServer.getServerUrl();

    // Get the actual repo config
    // But set a non-existing token name
    RepositoryConfig repoConfig =
        new RepositoryConfig.Builder(mockRepoConfig)
            .setLink(serverUrl + "ignored")
            .setAuth(NON_EXISTS_AUTH_CONFIG)
            .build();
    PushAppOperationRequest mockPushContext =
        new PushAppOperationRequest(new NamespaceId(NAMESPACE), repoConfig, mockAppDetails, mockCommit);

    RemoteSourceControlOperationRunner operationRunner =
      new RemoteSourceControlOperationRunner(cConf, metricsCollectionService, remoteClientFactory);

    operationRunner.push(mockPushContext);
  }

  @Test
  public void testPullSuccess() throws Exception {
    String serverUrl = gitServer.getServerUrl();
    RepositoryConfig repoConfig =
      new RepositoryConfig.Builder(mockRepoConfig).setLink(serverUrl + "ignored").build();
    PullAppOperationRequest mockPullRequest = new PullAppOperationRequest(mockAppRef, repoConfig);

    RemoteSourceControlOperationRunner operationRunner =
      new RemoteSourceControlOperationRunner(cConf, metricsCollectionService, remoteClientFactory);
    Path configFilePath = Paths.get(PATH_PREFIX, TEST_APP_NAME + ".json");

    addFileToGit(configFilePath, TEST_APP_SPEC, gitServer);

    PullAppResponse<?> response = operationRunner.pull(mockPullRequest);
    // Verify SecureSystemReader is being used.
    Assert.assertTrue(SystemReader.getInstance() instanceof SecureSystemReader);

    // validate pull response
    validatePullResponse(response, TEST_APP_NAME);
    Assert.assertEquals(response.getApplicationName(), mockAppDetails.getName());
  }

  @Test(expected = SourceControlAppConfigNotFoundException.class)
  public void testPullAppConfigNotFoundException() throws Exception {
    String serverUrl = gitServer.getServerUrl();
    RepositoryConfig repoConfig =
      new RepositoryConfig.Builder(mockRepoConfig).setLink(serverUrl + "ignored").build();
    PullAppOperationRequest mockPullRequest = new PullAppOperationRequest(mockAppRef, repoConfig);

    RemoteSourceControlOperationRunner operationRunner =
      new RemoteSourceControlOperationRunner(cConf, metricsCollectionService, remoteClientFactory);
    operationRunner.pull(mockPullRequest);
  }

  @Test(expected = SourceControlException.class)
  public void testPullFailureException() throws Exception {
    String serverUrl = gitServer.getServerUrl();
    RepositoryConfig repoConfig =
      new RepositoryConfig.Builder(mockRepoConfig).setLink(serverUrl + "ignored").build();
    PullAppOperationRequest mockPullRequest = new PullAppOperationRequest(mockAppRef, repoConfig);

    RemoteSourceControlOperationRunner operationRunner =
      new RemoteSourceControlOperationRunner(cConf, metricsCollectionService, remoteClientFactory);

    // close the remote repository simulating failure
    gitServer.after();
    operationRunner.pull(mockPullRequest);
  }

  @Test(expected = AuthenticationConfigException.class)
  public void testPullAuthenticationConfigException() throws Exception {
    String serverUrl = gitServer.getServerUrl();

    // Get the actual repo config
    // But set a non-existing token name
    RepositoryConfig repoConfig = new RepositoryConfig.Builder(mockRepoConfig)
        .setLink(serverUrl + "ignored")
        .setAuth(NON_EXISTS_AUTH_CONFIG)
        .build();
    PullAppOperationRequest mockPullRequest =
      new PullAppOperationRequest(mockAppRef, repoConfig);

    RemoteSourceControlOperationRunner operationRunner =
      new RemoteSourceControlOperationRunner(cConf, metricsCollectionService, remoteClientFactory);

    operationRunner.pull(mockPullRequest);
  }

  @Test
  public void testListSuccess() throws Exception {
    String serverUrl = gitServer.getServerUrl();
    RepositoryConfig repoConfig =
      new RepositoryConfig.Builder(mockRepoConfig).setLink(serverUrl + "ignored").build();
    NamespaceRepository testNamespaceRepository = new NamespaceRepository(new NamespaceId(NAMESPACE), repoConfig);

    RemoteSourceControlOperationRunner operationRunner =
      new RemoteSourceControlOperationRunner(cConf, metricsCollectionService, remoteClientFactory);

    RepositoryApp app1 = new RepositoryApp(TEST_APP_NAME, getGitStyleHash(TEST_APP_SPEC));
    Path configFile1 = Paths.get(PATH_PREFIX, app1.getName() + ".json");
    addFileToGit(configFile1, TEST_APP_SPEC, gitServer);

    RepositoryApp app2 = new RepositoryApp(TEST_APP_NAME + "1", getGitStyleHash(TEST_APP_SPEC));
    Path configFile2 = Paths.get(PATH_PREFIX, app2.getName() + ".json");
    addFileToGit(configFile2, TEST_APP_SPEC, gitServer);

    RepositoryAppsResponse response = operationRunner.list(testNamespaceRepository);
    // Verify SecureSystemReader is being used.
    Assert.assertTrue(SystemReader.getInstance() instanceof SecureSystemReader);
    List<RepositoryApp> apps = response.getApps().stream()
      .sorted(Comparator.comparing(RepositoryApp::getName)).collect(Collectors.toList());

    Assert.assertEquals(app1, apps.get(0));
    Assert.assertEquals(app2, apps.get(1));
  }

  @Test(expected = SourceControlException.class)
  public void testListSourceControlException() throws Exception {
    String serverUrl = gitServer.getServerUrl();
    RepositoryConfig repoConfig =
      new RepositoryConfig.Builder(mockRepoConfig).setLink(serverUrl + "ignored").build();
    NamespaceRepository testNamespaceRepository = new NamespaceRepository(new NamespaceId(NAMESPACE), repoConfig);

    RemoteSourceControlOperationRunner operationRunner =
      new RemoteSourceControlOperationRunner(cConf, metricsCollectionService, remoteClientFactory);

    RepositoryApp app1 = new RepositoryApp(TEST_APP_NAME, getGitStyleHash(TEST_APP_SPEC));
    Path configFile1 = Paths.get(PATH_PREFIX, app1.getName() + ".json");
    addFileToGit(configFile1, TEST_APP_SPEC, gitServer);

    // close the remote repository
    gitServer.after();
    operationRunner.list(testNamespaceRepository);
  }

  @Test(expected = NotFoundException.class)
  public void testListRepoPrefixNotFoundException() throws Exception {
    String serverUrl = gitServer.getServerUrl();
    RepositoryConfig repoConfig =
      new RepositoryConfig.Builder(mockRepoConfig).setLink(serverUrl + "ignored").build();
    NamespaceRepository testNamespaceRepository = new NamespaceRepository(new NamespaceId(NAMESPACE), repoConfig);

    RemoteSourceControlOperationRunner operationRunner =
      new RemoteSourceControlOperationRunner(cConf, metricsCollectionService, remoteClientFactory);

    operationRunner.list(testNamespaceRepository);
  }

  @Test(expected = AuthenticationConfigException.class)
  public void testListAuthenticationConfigException() throws Exception {
    String serverUrl = gitServer.getServerUrl();
    // Get the actual repo config
    // But set a non-existing token name
    RepositoryConfig repoConfig = new RepositoryConfig.Builder(mockRepoConfig)
        .setLink(serverUrl + "ignored")
        .setAuth(NON_EXISTS_AUTH_CONFIG)
        .build();
    NamespaceRepository testNamespaceRepository = new NamespaceRepository(new NamespaceId(NAMESPACE), repoConfig);

    RemoteSourceControlOperationRunner operationRunner =
      new RemoteSourceControlOperationRunner(cConf, metricsCollectionService, remoteClientFactory);

    operationRunner.list(testNamespaceRepository);
  }
}
