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

import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class SourceControlServiceTest extends AppFabricTestBase {
  private static CConfiguration cConf;
  private static SourceControlService sourceControlService;

  @BeforeClass
  public static void beforeClass() throws Exception {
    cConf = createBasicCConf();
    // we enable Kerberos for these unit tests, so we can test namespace group permissions (see testDataDirCreation).
    cConf.set(Constants.Security.KERBEROS_ENABLED, Boolean.toString(true));
    cConf.set(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL, "cdap");
    initializeAndStartServices(cConf);

    sourceControlService = getInjector().getInstance(SourceControlService.class);
  }


  @Test
  public void testSetRepoConfig() throws Exception {
    String namespace = "custompaceNamespace";
    NamespaceId namespaceId = new NamespaceId(namespace);
    RepositoryConfig namespaceRepo = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink("example.com").setDefaultBranch("develop").setAuthType(AuthType.PAT)
      .setTokenName("token").setUsername("user").build();
    sourceControlService.setRepository(namespaceId, namespaceRepo);
    Assert.assertNotNull(sourceControlService.getRepository(namespaceId));

    RepositoryConfig newRepositoryConfig = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink("another.example.com").setDefaultBranch("master").setAuthType(AuthType.PAT)
      .setTokenName("another.token").setUsername("another.user").build();
    sourceControlService.setRepository(namespaceId, newRepositoryConfig);

    RepositoryConfig updatedNamespaceMeta = sourceControlService.getRepository(namespaceId);
    Assert.assertEquals(newRepositoryConfig, updatedNamespaceMeta);

    // Setting a null RepositoryConfig returns 400
    try {
      sourceControlService.setRepository(namespaceId, null);
      Assert.fail();
    } catch (BadRequestException e) {
      //expected
    }

    // Setting a null RepositoryConfig Provider returns 400
    try {
      sourceControlService.setRepository(namespaceId,
                                         new RepositoryConfig.Builder(namespaceRepo).setProvider(null).build());
      Assert.fail();
    } catch (BadRequestException e) {
      //expected
    }

    // Setting a null RepositoryConfig Link returns 400
    try {
      sourceControlService.setRepository(namespaceId,
                                         new RepositoryConfig.Builder(namespaceRepo).setLink(null).build());
      Assert.fail();
    } catch (BadRequestException e) {
      //expected
    }

    // Setting a null RepositoryConfig DefaultBranch returns 400
    try {
      sourceControlService.setRepository(namespaceId,
                                         new RepositoryConfig.Builder(namespaceRepo).setDefaultBranch(null).build());
      Assert.fail();
    } catch (BadRequestException e) {
      //expected
    }

    // Setting a null RepositoryConfig TokenName returns 400
    try {
      sourceControlService.setRepository(namespaceId,
                                         new RepositoryConfig.Builder(namespaceRepo).setTokenName(null).build());
      Assert.fail();
    } catch (BadRequestException e) {
      //expected
    }

    // Setting a null RepositoryConfig AuthType returns 400
    try {
      sourceControlService.setRepository(namespaceId,
                                         new RepositoryConfig.Builder(namespaceRepo).setAuthType(null).build());
      Assert.fail();
    } catch (BadRequestException e) {
      //expected
    }

    //clean up
    sourceControlService.deleteRepository(namespaceId);
  }

  @Test
  public void testDeleteRepoConfig() throws Exception {
    String namespace = "custompaceNamespace";
    NamespaceId namespaceId = new NamespaceId(namespace);
    RepositoryConfig namespaceRepo = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink("example.com").setDefaultBranch("develop").setAuthType(AuthType.PAT)
      .setTokenName("token").setUsername("user").build();
    sourceControlService.setRepository(namespaceId, namespaceRepo);
    Assert.assertNotNull(sourceControlService.getRepository(namespaceId));
    RepositoryConfig namespaceRepository = sourceControlService.getRepository(namespaceId);
    Assert.assertEquals(namespaceRepo, namespaceRepository);

    sourceControlService.deleteRepository(namespaceId);

    namespaceRepository = sourceControlService.getRepository(namespaceId);

    Assert.assertNull(namespaceRepository);
  }
}
