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

import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.RepositoryNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryMeta;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for {@link SourceControlManagementService}
 */
public class SourceControlManagementServiceTest extends AppFabricTestBase {
  private static CConfiguration cConf;
  private static NamespaceAdmin namespaceAdmin;
  private static SourceControlManagementService sourceControlService;

  @BeforeClass
  public static void beforeClass() throws Exception {
    cConf = createBasicCConf();
    initializeAndStartServices(cConf);
    namespaceAdmin = getInjector().getInstance(NamespaceAdmin.class);
    sourceControlService = getInjector().getInstance(SourceControlManagementService.class);
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
    Assert.assertNotNull(repoMeta.getUpdatedTimeMillis());

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
}
