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
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
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
import io.cdap.cdap.sourcecontrol.SourceControlTestBase;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentDecompressor;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;

/**
 * Tests for {@link RemoteSourceControlOperationRunner}
 */
public class RemoteSourceControlOperationRunnerTest extends SourceControlTestBase {
  private static final ApplicationDetail mockAppDetails = new ApplicationDetail(
    "app1", "v1", "description1", null, null, "conf1", new ArrayList<>(),
    new ArrayList<>(), new ArrayList<>(), null, null);
  private static final CommitMeta mockCommit = new CommitMeta("author", "commiter", System.currentTimeMillis(),
                                                              "commit");
  private static final RepositoryConfig mockRepoConfig =
      new RepositoryConfig.Builder()
          .setProvider(Provider.GITHUB)
          .setLink("ignored")
          .setDefaultBranch(DEFAULT_BRANCH_NAME)
          .setPathPrefix("pathPrefix")
          .setAuthType(AuthType.PAT)
          .setTokenName(TOKEN_NAME)
          .build();
  private static CConfiguration cConf;
  private static SConfiguration sConf;
  private static NettyHttpService httpService;
  private static RemoteClientFactory remoteClientFactory;
  private static MetricsCollectionService metricsCollectionService;
  private static RemoteSecureStore remoteSecureStore;
  @ClassRule
  public static final TemporaryFolder TEST_TEMP_FOLDER = new TemporaryFolder();
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
    InMemoryNamespaceAdmin namespaceClient = new InMemoryNamespaceAdmin();

    NamespaceMeta namespaceMeta = new NamespaceMeta.Builder().setName(NAMESPACE).build();
    namespaceClient.create(namespaceMeta);

    FileSecureStoreService fileSecureStoreService = new FileSecureStoreService(cConf, sConf, namespaceClient,
                                                                               FileSecureStoreService.CURRENT_CODEC
                                                                                 .newInstance());
    
    httpService = new CommonNettyHttpServiceBuilder(cConf, "test", new NoOpMetricsCollectionService())
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
    remoteSecureStore.put(NAMESPACE, TOKEN_NAME, MOCK_TOKEN, "fake git token", ImmutableMap.of("prop1", "value1"));
  }

  @AfterClass
  public static void finish() throws Exception {
    httpService.stop();
  }

  @Test
  public void testRemoteSourceControlOperationRunner() throws Exception {
    // Get the actual repo config
    String serverURL = gitServer.getServerURL();
    RepositoryConfig repoConfig =
        new RepositoryConfig.Builder(mockRepoConfig).setLink(serverURL + "ignored").build();
    PushAppContext mockPushContext =
        new PushAppContext(new NamespaceId(NAMESPACE), repoConfig, mockAppDetails, mockCommit);

    RemoteSourceControlOperationRunner operationRunner =
      new RemoteSourceControlOperationRunner(cConf, metricsCollectionService, remoteClientFactory);

    PushAppResponse pushResponse = operationRunner.push(mockPushContext);

    // Assert the pushed app in response
    Assert.assertEquals(pushResponse.getName(), mockAppDetails.getName());
    Assert.assertEquals(pushResponse.getVersion(), mockAppDetails.getAppVersion());
  }

  @Test(expected = SourceControlException.class)
  public void testRemoteSourceControlOperationRunnerPushFailureException() throws Exception {
    // Get the actual repo config
    String serverURL = gitServer.getServerURL();
    RepositoryConfig repoConfig =
        new RepositoryConfig.Builder(mockRepoConfig).setLink(serverURL + "ignored").build();
    PushAppContext mockPushContext =
        new PushAppContext(new NamespaceId(NAMESPACE), repoConfig, mockAppDetails, mockCommit);

    RemoteSourceControlOperationRunner operationRunner =
      new RemoteSourceControlOperationRunner(cConf, metricsCollectionService, remoteClientFactory);

    // close the remote repository
    gitServer.after();
    operationRunner.push(mockPushContext);
  }

  @Test(expected = NoChangesToPushException.class)
  public void testRemoteSourceControlOperationRunnerNoChangesToPushException() throws Exception {
    // Get the actual repo config
    String serverURL = gitServer.getServerURL();
    RepositoryConfig repoConfig =
        new RepositoryConfig.Builder(mockRepoConfig).setLink(serverURL + "ignored").build();
    PushAppContext mockPushContext =
        new PushAppContext(new NamespaceId(NAMESPACE), repoConfig, mockAppDetails, mockCommit);

    RemoteSourceControlOperationRunner operationRunner =
      new RemoteSourceControlOperationRunner(cConf, metricsCollectionService, remoteClientFactory);

    operationRunner.push(mockPushContext);
    // Push the same unchanged application again
    operationRunner.push(mockPushContext);
  }

  @Test(expected = AuthenticationConfigException.class)
  public void testRemoteSourceControlOperationRunnerAuthenticationConfigException() throws Exception {
    String serverURL = gitServer.getServerURL();

    // Get the actual repo config
    // But set a non-existing token name
    RepositoryConfig repoConfig =
        new RepositoryConfig.Builder(mockRepoConfig)
            .setLink(serverURL + "ignored")
            .setTokenName(TOKEN_NAME + "not_exist")
            .build();
    PushAppContext mockPushContext =
        new PushAppContext(new NamespaceId(NAMESPACE), repoConfig, mockAppDetails, mockCommit);

    RemoteSourceControlOperationRunner operationRunner =
      new RemoteSourceControlOperationRunner(cConf, metricsCollectionService, remoteClientFactory);

    operationRunner.push(mockPushContext);
  }
}
