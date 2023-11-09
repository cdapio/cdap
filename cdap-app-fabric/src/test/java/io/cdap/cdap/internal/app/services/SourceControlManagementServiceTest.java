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

package io.cdap.cdap.internal.app.services;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import io.cdap.cdap.ConfigTestApp;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.RepositoryNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.metadata.MetadataSubscriberService;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.AuthConfig;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.PatConfig;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryMeta;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.sourcecontrol.ApplicationManager;
import io.cdap.cdap.sourcecontrol.AuthenticationConfigException;
import io.cdap.cdap.sourcecontrol.NoChangesToPullException;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.SourceControlException;
import io.cdap.cdap.sourcecontrol.operationrunner.MultiPullAppOperationRequest;
import io.cdap.cdap.sourcecontrol.operationrunner.MultiPushAppOperationRequest;
import io.cdap.cdap.sourcecontrol.operationrunner.NamespaceRepository;
import io.cdap.cdap.sourcecontrol.operationrunner.PullAppOperationRequest;
import io.cdap.cdap.sourcecontrol.operationrunner.PullAppResponse;
import io.cdap.cdap.sourcecontrol.operationrunner.PushAppOperationRequest;
import io.cdap.cdap.sourcecontrol.operationrunner.PushAppResponse;
import io.cdap.cdap.sourcecontrol.operationrunner.RepositoryApp;
import io.cdap.cdap.sourcecontrol.operationrunner.RepositoryAppsResponse;
import io.cdap.cdap.sourcecontrol.operationrunner.SourceControlOperationRunner;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link SourceControlManagementService}.
 */
public class SourceControlManagementServiceTest extends AppFabricTestBase {

  private static CConfiguration cConf;
  private static NamespaceAdmin namespaceAdmin;
  private static SourceControlManagementService sourceControlService;
  private static final RepositoryConfig REPOSITORY_CONFIG = new RepositoryConfig.Builder()
      .setProvider(Provider.GITHUB)
      .setLink("example.com")
      .setDefaultBranch("develop")
      .setAuth(new AuthConfig(AuthType.PAT, new PatConfig("passwordName", "user")))
      .build();
  private static final SourceControlOperationRunner sourceControlOperationRunnerSpy =
      Mockito.spy(new MockSourceControlOperationRunner());

  @BeforeClass
  public static void beforeClass() throws Exception {
    cConf = createBasicCConf();
    initializeAndStartServices(cConf);
    namespaceAdmin = getInjector().getInstance(NamespaceAdmin.class);
    sourceControlService =
        getInjector().getInstance(SourceControlManagementService.class);
  }

  protected static void initializeAndStartServices(CConfiguration cConf) throws Exception {
    initializeAndStartServices(cConf, new AbstractModule() {
      @Override
      protected void configure() {
        bind(UGIProvider.class).to(CurrentUGIProvider.class);
        bind(MetadataSubscriberService.class).in(Scopes.SINGLETON);
        bind(SourceControlOperationRunner.class).toInstance(sourceControlOperationRunnerSpy);
      }
    });
  }

  @Test
  public void testSetRepoConfig() throws Exception {
    String namespace = "customNamespace";
    NamespaceId namespaceId = new NamespaceId(namespace);

    try {
      sourceControlService.setRepository(namespaceId, REPOSITORY_CONFIG);
      Assert.fail();
    } catch (NamespaceNotFoundException e) {
      // no-op
      // Setting repository will fail since the namespace does not exist
    }

    // Create namespace and repository should succeed
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(namespace).build());
    sourceControlService.setRepository(namespaceId, REPOSITORY_CONFIG);

    RepositoryMeta repoMeta = sourceControlService.getRepositoryMeta(namespaceId);
    Assert.assertEquals(REPOSITORY_CONFIG, repoMeta.getConfig());
    Assert.assertNotEquals(0, repoMeta.getUpdatedTimeMillis());

    RepositoryConfig newRepositoryConfig = new RepositoryConfig.Builder(REPOSITORY_CONFIG)
        .setLink("another.example.com")
        .setDefaultBranch("master")
        .setAuth(new AuthConfig(AuthType.PAT, new PatConfig("another.password", "another.user")))
        .build();
    sourceControlService.setRepository(namespaceId, newRepositoryConfig);

    // Verify repository updated
    repoMeta = sourceControlService.getRepositoryMeta(namespaceId);
    Assert.assertEquals(newRepositoryConfig, repoMeta.getConfig());
    Assert.assertNotEquals(0, repoMeta.getUpdatedTimeMillis());

    //clean up
    namespaceAdmin.delete(namespaceId);

    try {
      sourceControlService.getRepositoryMeta(namespaceId);
      Assert.fail();
    } catch (RepositoryNotFoundException e) {
      // no-op
    }
  }

  @Test
  public void testDeleteRepoConfig() throws Exception {
    String namespace = "customNamespace";
    NamespaceId namespaceId = new NamespaceId(namespace);

    try {
      sourceControlService.setRepository(namespaceId, REPOSITORY_CONFIG);
      Assert.fail();
    } catch (NamespaceNotFoundException e) {
      // no-op
      // Setting repository will fail since the namespace does not exist
    }

    // Create namespace and repository should succeed
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(namespace).build());
    sourceControlService.setRepository(namespaceId, REPOSITORY_CONFIG);

    RepositoryMeta repoMeta = sourceControlService.getRepositoryMeta(namespaceId);
    Assert.assertEquals(REPOSITORY_CONFIG, repoMeta.getConfig());

    // Delete repository and verify it's deleted
    sourceControlService.deleteRepository(namespaceId);

    try {
      sourceControlService.getRepositoryMeta(namespaceId);
      Assert.fail();
    } catch (RepositoryNotFoundException e) {
      // no-op
    }

    //clean up
    namespaceAdmin.delete(namespaceId);
  }

  @Test
  public void testPushAppSucceeds() throws Exception {
    // Deploy one application in default namespace
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp");
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "appWithConfig",
        "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, ConfigTestApp.class);

    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    deploy(appId1, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId()), config));

    // Set the repository config
    String namespace = Id.Namespace.DEFAULT.getId();
    NamespaceId namespaceId = new NamespaceId(namespace);

    sourceControlService.setRepository(namespaceId, REPOSITORY_CONFIG);

    PushAppResponse expectedAppResponse = new PushAppResponse(appId1.getId(), appId1.getVersion(),
        appId1.getId() + " hash");

    Mockito.doReturn(expectedAppResponse).when(sourceControlOperationRunnerSpy).push(Mockito.any());

    // Assert the result is as expected
    PushAppResponse result = sourceControlService.pushApp(namespaceId.appReference(appId1.getId()),
        "some commit");

    Assert.assertEquals(result, expectedAppResponse);

    // Assert the source control meta field is updated
    ApplicationDetail appDetail = getAppDetails(Id.Namespace.DEFAULT.getId(), appId1.getId());
    SourceControlMeta metaFromPushResult = new SourceControlMeta(result.getFileHash());
    Assert.assertEquals(appDetail.getSourceControlMeta(), metaFromPushResult);

    // Cleanup
    deleteApp(appId1, 200);
    deleteArtifact(artifactId, 200);
    sourceControlService.deleteRepository(namespaceId);
  }

  @Test
  public void testPushAppsRepoNotFoundException() throws Exception {
    // Deploy one application in default namespace
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp");
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "appWithConfig", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, ConfigTestApp.class);

    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    deploy(appId1, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId()), config));

    // Do not set the repository config
    NamespaceId namespaceId = new NamespaceId(Id.Namespace.DEFAULT.getId());

    PushAppResponse expectedAppResponse = new PushAppResponse(appId1.getId(), appId1.getVersion(),
                                                              appId1.getId() + " hash");

    Mockito.doReturn(expectedAppResponse).when(sourceControlOperationRunnerSpy).push(Mockito.any());

    // Assert the result is as expected
    try {
      sourceControlService.pushApp(namespaceId.appReference(appId1.getId()), "some commit");
      Assert.fail();
    } catch (RepositoryNotFoundException e) {
      // no-op
    }

    // Cleanup
    deleteApp(appId1, 200);
    deleteArtifact(artifactId, 200);
  }

  @Test(expected = NotFoundException.class)
  public void testPushAppsApplicationNotFoundException() throws Exception {
    NamespaceId namespaceId = new NamespaceId(Id.Namespace.DEFAULT.getId());
    sourceControlService.pushApp(namespaceId.appReference("appNotFound"), "some commit");
  }

  @Test
  public void testPushAppsSourceControlException() throws Exception {
    // Deploy one application in default namespace
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp");
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "appWithConfig", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, ConfigTestApp.class);

    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    deploy(appId1, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId()), config));

    // Do not set the repository config
    NamespaceId namespaceId = new NamespaceId(Id.Namespace.DEFAULT.getId());

    // Set the repository config
    sourceControlService.setRepository(namespaceId, REPOSITORY_CONFIG);

    Mockito.doThrow(new SourceControlException("push apps failed", new Exception()))
      .when(sourceControlOperationRunnerSpy).push(Mockito.any());

    // Assert the result is as expected
    try {
      sourceControlService.pushApp(namespaceId.appReference(appId1.getId()), "some commit");
      Assert.fail();
    } catch (SourceControlException e) {
      // no-op
    }

    // Cleanup
    deleteApp(appId1, 200);
    deleteArtifact(artifactId, 200);
  }

  @Test
  public void testPullAndDeployAppSucceeds() throws Exception {
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp");

    // Deploy app artifact in default namespace
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "appWithConfig", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, ConfigTestApp.class);
    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    AppRequest<?> mockAppRequest = new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId()), config);

    // Set the repository config
    String namespace = Id.Namespace.DEFAULT.getId();
    NamespaceId namespaceId = new NamespaceId(namespace);

    sourceControlService.setRepository(namespaceId, REPOSITORY_CONFIG);

    PullAppResponse<?> expectedPullResponse = new PullAppResponse(appId1.getId(),
        appId1.getId() + " " + "hash", mockAppRequest);
    Mockito.doReturn(expectedPullResponse).when(sourceControlOperationRunnerSpy)
        .pull(Mockito.any(PullAppOperationRequest.class));

    // Assert the result is as expected
    ApplicationRecord result = sourceControlService.pullAndDeploy(namespaceId.appReference(appId1.getId()));

    Assert.assertNotNull(result.getSourceControlMeta());
    Assert.assertEquals(result.getSourceControlMeta().getFileHash(), expectedPullResponse.getApplicationFileHash());
    Assert.assertEquals(result.getName(), appId1.getId());

    // Assert the source control meta field is updated
    ApplicationDetail appDetail = getAppDetails(Id.Namespace.DEFAULT.getId(), appId1.getId());
    SourceControlMeta metaFromPushResult = new SourceControlMeta(expectedPullResponse.getApplicationFileHash());
    Assert.assertEquals(appDetail.getSourceControlMeta(), metaFromPushResult);

    // Cleanup
    deleteApp(appId1, 200);
    deleteArtifact(artifactId, 200);
    sourceControlService.deleteRepository(namespaceId);
  }

  @Test(expected = RepositoryNotFoundException.class)
  public void testPullAndDeployRepoNotFoundException() throws Exception {
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp");
    // Deploy app artifact in default namespace
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "appWithConfig", "1.0.0-SNAPSHOT");
    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    AppRequest<?> mockAppRequest = new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId()), config);

    // Do not set the repository config
    NamespaceId namespaceId = new NamespaceId(Id.Namespace.DEFAULT.getId());
    PullAppResponse<?> expectedPullResponse = new PullAppResponse(appId1.getId(),
        appId1.getId() + " hash", mockAppRequest);
    Mockito.doReturn(expectedPullResponse).when(sourceControlOperationRunnerSpy)
        .pull(Mockito.any(PullAppOperationRequest.class));

    sourceControlService.pullAndDeploy(namespaceId.appReference(appId1.getId()));
  }

  @Test(expected = NotFoundException.class)
  public void testPullAndDeployAppNotFoundException() throws Exception {
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp");
    String namespace = Id.Namespace.DEFAULT.getId();
    NamespaceId namespaceId = new NamespaceId(namespace);
    ApplicationReference appRef = namespaceId.appReference(appId1.getId());

    // Set the repository config
    sourceControlService.setRepository(namespaceId, REPOSITORY_CONFIG);

    Mockito.doThrow(new ApplicationNotFoundException(appRef))
        .when(sourceControlOperationRunnerSpy).pull(Mockito.any(PullAppOperationRequest.class));

    try {
      sourceControlService.pullAndDeploy(appRef);
    } finally {
      sourceControlService.deleteRepository(namespaceId);
    }
  }

  @Test(expected = AuthenticationConfigException.class)
  public void testPullAndDeployAuthenticationConfigException() throws Exception {
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp");
    String namespace = Id.Namespace.DEFAULT.getId();
    NamespaceId namespaceId = new NamespaceId(namespace);
    ApplicationReference appRef = namespaceId.appReference(appId1.getId());

    // Set the repository config
    sourceControlService.setRepository(namespaceId, REPOSITORY_CONFIG);

    Mockito.doThrow(new AuthenticationConfigException("Repo config is invalid"))
        .when(sourceControlOperationRunnerSpy).pull(Mockito.any(PullAppOperationRequest.class));

    try {
      sourceControlService.pullAndDeploy(appRef);
    } finally {
      sourceControlService.deleteRepository(namespaceId);
    }
  }

  @Test(expected = SourceControlException.class)
  public void testPullAndDeploySourceControlException() throws Exception {
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp");
    String namespace = Id.Namespace.DEFAULT.getId();
    NamespaceId namespaceId = new NamespaceId(namespace);
    ApplicationReference appRef = namespaceId.appReference(appId1.getId());

    // Set the repository config
    sourceControlService.setRepository(namespaceId, REPOSITORY_CONFIG);

    Mockito.doThrow(new SourceControlException("Failed to pull application", new Exception()))
        .when(sourceControlOperationRunnerSpy).pull(Mockito.any(PullAppOperationRequest.class));

    try {
      sourceControlService.pullAndDeploy(appRef);
    } finally {
      sourceControlService.deleteRepository(namespaceId);
    }
  }

  @Test
  public void testPullAndDeployNoChangesToPullException() throws Exception {
    // Deploy one application in default namespace
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp");
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "appWithConfig", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, ConfigTestApp.class);
    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    AppRequest<?> mockAppRequest = new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId()), config);
    deploy(appId1, mockAppRequest);

    // Set the repository config
    String namespace = Id.Namespace.DEFAULT.getId();
    NamespaceId namespaceId = new NamespaceId(namespace);
    sourceControlService.setRepository(namespaceId, REPOSITORY_CONFIG);

    // Push the application and update source control metadata
    String mockedFileHash = appId1.getId() + " hash";
    PushAppResponse expectedAppResponse = new PushAppResponse(appId1.getId(), appId1.getVersion(), mockedFileHash);
    Mockito.doReturn(expectedAppResponse).when(sourceControlOperationRunnerSpy).push(Mockito.any());
    sourceControlService.pushApp(namespaceId.appReference(appId1.getId()), "some commit");

    // Set up the pullResponse so that the fileHashes are the same
    PullAppResponse<?> expectedPullResponse = new PullAppResponse(appId1.getId(), mockedFileHash,
        mockAppRequest);
    Mockito.doReturn(expectedPullResponse).when(sourceControlOperationRunnerSpy)
        .pull(Mockito.any(PullAppOperationRequest.class));
    try {
      ApplicationReference appRef = namespaceId.appReference(appId1.getId());
      sourceControlService.pullAndDeploy(appRef);
      Assert.fail();
    } catch (NoChangesToPullException e) {
      // no-op
    }

    // Cleanup
    deleteApp(appId1, 200);
    deleteArtifact(artifactId, 200);
    sourceControlService.deleteRepository(namespaceId);
  }

  @Test
  public void testListAppsSucceed() throws Exception {
    RepositoryApp app1 = new RepositoryApp("app1", "hash1");
    RepositoryApp app2 = new RepositoryApp("app2", "hash2");
    RepositoryAppsResponse expectedListResult = new RepositoryAppsResponse(Arrays.asList(app1, app2));
    NamespaceId namespaceId = new NamespaceId(Id.Namespace.DEFAULT.getId());
    sourceControlService.setRepository(namespaceId, REPOSITORY_CONFIG);

    Mockito.doReturn(expectedListResult)
      .when(sourceControlOperationRunnerSpy).list(Mockito.any(NamespaceRepository.class));

    RepositoryAppsResponse result = sourceControlService.listApps(namespaceId);
    List<RepositoryApp> actualAppsList = result.getApps().stream()
        .sorted(Comparator.comparing(RepositoryApp::getName)).collect(Collectors.toList());

    Assert.assertEquals(actualAppsList, expectedListResult.getApps());
  }

  @Test(expected = SourceControlException.class)
  public void testListAppsFailed() throws Exception {
    NamespaceId namespaceId = new NamespaceId(Id.Namespace.DEFAULT.getId());
    sourceControlService.setRepository(namespaceId, REPOSITORY_CONFIG);

    Mockito.doThrow(SourceControlException.class)
      .when(sourceControlOperationRunnerSpy).list(Mockito.any(NamespaceRepository.class));

    sourceControlService.listApps(namespaceId);
  }

  @Test
  public void testOperationRunnerStarted() {
    Assert.assertTrue(sourceControlOperationRunnerSpy.isRunning());
  }

  /**
   * A Mock {@link SourceControlOperationRunner} that can be used for tests.
   */
  private static class MockSourceControlOperationRunner extends
      AbstractIdleService implements
      SourceControlOperationRunner {

    @Override
    public PushAppResponse push(PushAppOperationRequest pushAppOperationRequest)
        throws NoChangesToPushException, AuthenticationConfigException {
      return null;
    }

    @Override
    public List<PushAppResponse> multiPush(MultiPushAppOperationRequest pushRequest,
        ApplicationManager appManager)
        throws NoChangesToPushException, AuthenticationConfigException {
      return null;
    }

    @Override
    public PullAppResponse<?> pull(PullAppOperationRequest pullRequest)
        throws NotFoundException, AuthenticationConfigException {
      return null;
    }

    @Override
    public void multiPull(MultiPullAppOperationRequest pullRequest, Consumer<PullAppResponse<?>> consumer)
        throws NotFoundException, AuthenticationConfigException {
    }

    @Override
    public RepositoryAppsResponse list(NamespaceRepository nameSpaceRepository)
        throws AuthenticationConfigException, NotFoundException {
      return null;
    }

    @Override
    protected void startUp() throws Exception {
      // no-op.
    }

    @Override
    protected void shutDown() throws Exception {
      // no-op.
    }
  }
}
