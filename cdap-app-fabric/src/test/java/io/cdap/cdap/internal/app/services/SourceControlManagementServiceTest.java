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
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.RepositoryNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.metadata.MetadataSubscriberService;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryMeta;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.sourcecontrol.AuthenticationConfigException;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.operationrunner.PushAppResponse;
import io.cdap.cdap.sourcecontrol.operationrunner.PushFailureException;
import io.cdap.cdap.sourcecontrol.operationrunner.SourceControlOperationRunner;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link SourceControlManagementService}
 */
public class SourceControlManagementServiceTest extends AppFabricTestBase {
  private static CConfiguration cConf;
  private static NamespaceAdmin namespaceAdmin;
  private static SourceControlManagementService sourceControlService;
  private static final SourceControlOperationRunner mockSourceControlOperationRunner =
    Mockito.mock(SourceControlOperationRunner.class);

  @BeforeClass
  public static void beforeClass() throws Exception {
    cConf = createBasicCConf();
    initializeAndStartServices(cConf);
    namespaceAdmin = getInjector().getInstance(NamespaceAdmin.class);
    sourceControlService = getInjector().getInstance(SourceControlManagementService.class);
  }

  protected static void initializeAndStartServices(CConfiguration cConf) throws Exception {
    initializeAndStartServices(cConf, new AbstractModule() {
      @Override
      protected void configure() {
        bind(UGIProvider.class).to(CurrentUGIProvider.class);
        bind(MetadataSubscriberService.class).in(Scopes.SINGLETON);
        bind(SourceControlOperationRunner.class).toInstance(mockSourceControlOperationRunner);
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
  public void testPushAppSucceeds() throws Exception {
    // Deploy one application in default namespace
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp");
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "appWithConfig", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, ConfigTestApp.class);

    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    deploy(appId1, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId()), config));

    // Set the repository config
    String namespace = Id.Namespace.DEFAULT.getId();
    NamespaceId namespaceId = new NamespaceId(namespace);
    RepositoryConfig namespaceRepo = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink("example.com").setDefaultBranch("develop").setAuthType(AuthType.PAT)
      .setTokenName("token").setUsername("user").build();
    sourceControlService.setRepository(namespaceId, namespaceRepo);
    
    PushAppResponse expectedAppResponse = new PushAppResponse(appId1.getId(), appId1.getVersion(),
                                                              appId1.getId() + " hash");

    Mockito.doReturn(expectedAppResponse)
      .when(mockSourceControlOperationRunner).push(Mockito.any(), Mockito.any(), Mockito.any());

    // Assert the result is as expected
    PushAppResponse result = sourceControlService.pushApp(namespaceId.appReference(appId1.getId()), "some commit");

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

    Mockito.doThrow(new RepositoryNotFoundException(namespaceId))
      .when(mockSourceControlOperationRunner).push(Mockito.any(), Mockito.any(), Mockito.any());

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

  @Test
  public void testPushAppNoChangesToPushException() throws Exception {
    // Deploy one application in default namespace
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp");
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "appWithConfig", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, ConfigTestApp.class);

    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    deploy(appId1, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId()), config));

    // Do not set the repository config
    NamespaceId namespaceId = new NamespaceId(Id.Namespace.DEFAULT.getId());

    Mockito.doThrow(new NoChangesToPushException("no changes to be pushed"))
      .when(mockSourceControlOperationRunner).push(Mockito.any(), Mockito.any(), Mockito.any());

    // Assert the result is as expected
    try {
      sourceControlService.pushApp(namespaceId.appReference(appId1.getId()), "some commit");
      Assert.fail();
    } catch (NoChangesToPushException e) {
      // no-op
    }

    // Cleanup
    deleteApp(appId1, 200);
    deleteArtifact(artifactId, 200);
  }

  @Test
  public void testPushAppAuthenticationConfigExceptionException() throws Exception {
    // Deploy one application in default namespace
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp");
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "appWithConfig", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, ConfigTestApp.class);

    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    deploy(appId1, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId()), config));

    // Do not set the repository config
    NamespaceId namespaceId = new NamespaceId(Id.Namespace.DEFAULT.getId());

    Mockito.doThrow(new AuthenticationConfigException("not able to get password from secure store"))
      .when(mockSourceControlOperationRunner).push(Mockito.any(), Mockito.any(), Mockito.any());

    // Assert the result is as expected
    try {
      sourceControlService.pushApp(namespaceId.appReference(appId1.getId()), "some commit");
      Assert.fail();
    } catch (AuthenticationConfigException e) {
      // no-op
    }

    // Cleanup
    deleteApp(appId1, 200);
    deleteArtifact(artifactId, 200);
  }

  @Test(expected = ApplicationNotFoundException.class)
  public void testPushAppsApplicationNotFoundException() throws Exception {
    NamespaceId namespaceId = new NamespaceId(Id.Namespace.DEFAULT.getId());
    sourceControlService.pushApp(namespaceId.appReference("appNotFound"), "some commit");
  }

  @Test
  public void testPushAppsPushFailureException() throws Exception {
    // Deploy one application in default namespace
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp");
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "appWithConfig", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, ConfigTestApp.class);

    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    deploy(appId1, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId()), config));

    // Do not set the repository config
    NamespaceId namespaceId = new NamespaceId(Id.Namespace.DEFAULT.getId());
    RepositoryConfig namespaceRepo = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink("example.com").setDefaultBranch("develop").setAuthType(AuthType.PAT)
      .setTokenName("token").setUsername("user").build();

    // Set the repository config
    sourceControlService.setRepository(namespaceId, namespaceRepo);

    Mockito.doThrow(new PushFailureException("push apps failed", new Exception()))
      .when(mockSourceControlOperationRunner).push(Mockito.any(), Mockito.any(), Mockito.any());

    // Assert the result is as expected
    try {
      sourceControlService.pushApp(namespaceId.appReference(appId1.getId()), "some commit");
      Assert.fail();
    } catch (PushFailureException e) {
      // no-op
    }

    // Cleanup
    deleteApp(appId1, 200);
    deleteArtifact(artifactId, 200);
  }
}
