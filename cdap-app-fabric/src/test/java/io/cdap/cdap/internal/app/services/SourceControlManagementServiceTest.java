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

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import io.cdap.cdap.ConfigTestApp;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.RepositoryNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.sourcecontrol.PushAppResponse;
import io.cdap.cdap.internal.app.sourcecontrol.PushAppsResponse;
import io.cdap.cdap.internal.app.sourcecontrol.PushFailureException;
import io.cdap.cdap.internal.app.sourcecontrol.SourceControlOperationRunner;
import io.cdap.cdap.internal.app.sourcecontrol.SourceControlOperationRunnerFactory;
import io.cdap.cdap.metadata.MetadataSubscriberService;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.BatchApplicationDetail;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryMeta;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.UGIProvider;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tests for {@link SourceControlManagementService}
 */
public class SourceControlManagementServiceTest extends AppFabricTestBase {
  private static CConfiguration cConf;
  private static NamespaceAdmin namespaceAdmin;
  private static SourceControlManagementService sourceControlService;
  private static final SourceControlOperationRunnerFactory mockSourceControlFactory =
    Mockito.mock(SourceControlOperationRunnerFactory.class);
  private static final SourceControlOperationRunner mockSourceControlOperationRunner =
    Mockito.mock(SourceControlOperationRunner.class);

  @BeforeClass
  public static void beforeClass() throws Exception {
    cConf = createBasicCConf();
    initializeAndStartServices(cConf);
    Mockito.doReturn(mockSourceControlOperationRunner).when(mockSourceControlFactory).create(Mockito.any());
    namespaceAdmin = getInjector().getInstance(NamespaceAdmin.class);
    sourceControlService = getInjector().getInstance(SourceControlManagementService.class);
  }

  protected static void initializeAndStartServices(CConfiguration cConf) throws Exception {
    initializeAndStartServices(cConf, new AbstractModule() {
      @Override
      protected void configure() {
        bind(UGIProvider.class).to(CurrentUGIProvider.class);
        bind(MetadataSubscriberService.class).in(Scopes.SINGLETON);
        bind(SourceControlOperationRunnerFactory.class).toInstance(mockSourceControlFactory);
      }
    });
  }

  @Test
  public void testSetRepoConfig() throws Exception {
    String namespace = "custompaceNamespace";
    NamespaceId namespaceId = new NamespaceId(namespace);
    RepositoryConfig namespaceRepo = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink("example.com").setDefaultBranch("develop").setAuthType(AuthType.PAT)
      .setTokenName("token").setUsername("user").build();
    
    try {
      sourceControlService.setRepository(namespaceId, namespaceRepo);
      Assert.fail();
    } catch (NamespaceNotFoundException e) {
      // no-op
      // Setting repository will fail since the namespace does not exist
    }

    // Create namespace and repository should succeed
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(namespace).build());
    sourceControlService.setRepository(namespaceId, namespaceRepo);

    RepositoryMeta repoMeta = sourceControlService.getRepositoryMeta(namespaceId);
    Assert.assertEquals(namespaceRepo, repoMeta.getConfig());
    Assert.assertNotEquals(0, repoMeta.getUpdatedTimeMillis());

    RepositoryConfig newRepositoryConfig = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink("another.example.com").setDefaultBranch("master").setAuthType(AuthType.PAT)
      .setTokenName("another.token").setUsername("another.user").build();
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
    String namespace = "custompaceNamespace";
    NamespaceId namespaceId = new NamespaceId(namespace);
    RepositoryConfig namespaceRepo = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink("example.com").setDefaultBranch("develop").setAuthType(AuthType.PAT)
      .setTokenName("token").setUsername("user").build();
    try {
      sourceControlService.setRepository(namespaceId, namespaceRepo);
      Assert.fail();
    } catch (NamespaceNotFoundException e) {
      // no-op
      // Setting repository will fail since the namespace does not exist
    }

    // Create namespace and repository should succeed
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(namespace).build());
    sourceControlService.setRepository(namespaceId, namespaceRepo);

    RepositoryMeta repoMeta = sourceControlService.getRepositoryMeta(namespaceId);
    Assert.assertEquals(namespaceRepo, repoMeta.getConfig());

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
  public void testPushAppsSucceed() throws Exception {
    // Deploy two applications in default namespace
    Collection<ImmutablePair<String, String>> appVersions = new ArrayList<>();
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp");
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "appWithConfig", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, ConfigTestApp.class);

    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    deploy(appId1, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId()), config));
    ApplicationDetail appDetail = getAppDetails(Id.Namespace.DEFAULT.getId(), appId1.getId());
    appVersions.add(new ImmutablePair<>(appId1.getId(), appDetail.getAppVersion()));

    Id.Application appId2 = Id.Application.from(Id.Namespace.DEFAULT, "AnotherApp");
    deploy(appId2, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId()), config));
    appDetail = getAppDetails(Id.Namespace.DEFAULT.getId(), appId2.getId());
    appVersions.add(new ImmutablePair<>(appId2.getId(), appDetail.getAppVersion()));

    // Set the repository config
    String namespace = Id.Namespace.DEFAULT.getId();
    NamespaceId namespaceId = new NamespaceId(namespace);
    RepositoryConfig namespaceRepo = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink("example.com").setDefaultBranch("develop").setAuthType(AuthType.PAT)
      .setTokenName("token").setUsername("user").build();
    sourceControlService.setRepository(namespaceId, namespaceRepo);
    
    List<ApplicationId> appIds = appVersions.stream()
      .map(app -> namespaceId.app(app.getFirst(), app.getSecond()))
      .collect(Collectors.toList());
    
    List<PushAppResponse> expectedAppsResponse = appIds.stream()
      .map(appId -> new PushAppResponse(appId.getApplication(), appId.getVersion(),
                                        appId.getApplication() + " hash"))
      .collect(Collectors.toList());

    Mockito.doReturn(new PushAppsResponse(expectedAppsResponse))
      .when(mockSourceControlOperationRunner).push(Mockito.any(), Mockito.any());

    // Assert the result is as expected
    PushAppsResponse result = sourceControlService.pushApps(namespaceId, appIds, "some commit");

    Assert.assertEquals(result.getApps(), expectedAppsResponse);

    // Assert the source control meta field is updated
    List<BatchApplicationDetail> appDetails = getAppDetails(Id.Namespace.DEFAULT.getId(), appVersions);
    Set<SourceControlMeta> metaInAppDetails =
      appDetails.stream().map(meta -> {
        Assert.assertNotNull(meta.getDetail());
        return meta.getDetail().getSourceControlMeta();
      }).collect(Collectors.toSet());
    Set<SourceControlMeta> metaFromPushResult = result.getApps().stream()
      .map(app -> new SourceControlMeta(app.getFileHash())).collect(Collectors.toSet());
    Assert.assertEquals(metaFromPushResult, metaInAppDetails);

    // Cleanup
    deleteApp(appId1, 200);
    deleteApp(appId2, 200);
    deleteArtifact(artifactId, 200);
    sourceControlService.deleteRepository(namespaceId);
  }

  @Test
  public void testPushAppsRepoNotFoundException() throws Exception {
    // Deploy one application in default namespace
    Collection<ImmutablePair<String, String>> appVersions = new ArrayList<>();
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp");
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "appWithConfig", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, ConfigTestApp.class);

    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    deploy(appId1, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId()), config));
    ApplicationDetail appDetail = getAppDetails(Id.Namespace.DEFAULT.getId(), appId1.getId());
    appVersions.add(new ImmutablePair<>(appId1.getId(), appDetail.getAppVersion()));

    // Do not set the repository config
    NamespaceId namespaceId = new NamespaceId(Id.Namespace.DEFAULT.getId());

    List<ApplicationId> appIds = appVersions.stream()
      .map(app -> namespaceId.app(app.getFirst(), app.getSecond()))
      .collect(Collectors.toList());

    List<PushAppResponse> expectedAppsResponse = appIds.stream()
      .map(appId -> new PushAppResponse(appId.getApplication(), appId.getVersion(),
                                        appId.getApplication() + " hash"))
      .collect(Collectors.toList());

    Mockito.doReturn(new PushAppsResponse(expectedAppsResponse))
      .when(mockSourceControlOperationRunner).push(Mockito.any(), Mockito.any());

    // Assert the result is as expected
    try {
      sourceControlService.pushApps(namespaceId, appIds, "some commit");
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
    String namespace = Id.Namespace.DEFAULT.getId();
    NamespaceId namespaceId = new NamespaceId(namespace);
    List<ApplicationId> apps = Arrays.asList(new ApplicationId(namespace, "app1", "versionId1"),
                                             new ApplicationId(namespace, "app2", "versionId2"));
    sourceControlService.pushApps(namespaceId, apps, "some commit");
  }

  @Test
  public void testPushAppsPushFailureException() throws Exception {
    // Deploy one application in default namespace
    Collection<ImmutablePair<String, String>> appVersions = new ArrayList<>();
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp");
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "appWithConfig", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, ConfigTestApp.class);

    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    deploy(appId1, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId()), config));
    ApplicationDetail appDetail = getAppDetails(Id.Namespace.DEFAULT.getId(), appId1.getId());
    appVersions.add(new ImmutablePair<>(appId1.getId(), appDetail.getAppVersion()));

    // Do not set the repository config
    NamespaceId namespaceId = new NamespaceId(Id.Namespace.DEFAULT.getId());
    RepositoryConfig namespaceRepo = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink("example.com").setDefaultBranch("develop").setAuthType(AuthType.PAT)
      .setTokenName("token").setUsername("user").build();

    // Set the repository config
    sourceControlService.setRepository(namespaceId, namespaceRepo);

    List<ApplicationId> appIds = appVersions.stream()
      .map(app -> namespaceId.app(app.getFirst(), app.getSecond()))
      .collect(Collectors.toList());

    Mockito.doThrow(new PushFailureException("push apps failed", new Exception()))
      .when(mockSourceControlOperationRunner).push(Mockito.any(), Mockito.any());

    // Assert the result is as expected
    try {
      sourceControlService.pushApps(namespaceId, appIds, "some commit");
      Assert.fail();
    } catch (PushFailureException e) {
      // no-op
    }

    // Cleanup
    deleteApp(appId1, 200);
    deleteArtifact(artifactId, 200);
  }
}
