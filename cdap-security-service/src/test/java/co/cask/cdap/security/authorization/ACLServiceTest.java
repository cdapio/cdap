/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.security.authorization;

import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.security.ACL;
import co.cask.cdap.api.security.EntityId;
import co.cask.cdap.api.security.EntityType;
import co.cask.cdap.api.security.PermissionType;
import co.cask.cdap.api.security.Principal;
import co.cask.cdap.api.security.PrincipalType;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.InMemoryDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.ACLTableModule;
import co.cask.cdap.data2.dataset2.lib.table.CoreDatasetsModule;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryMetricsTableModule;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryOrderedTableModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Test for {@link ACLService}.
 */
public class ACLServiceTest {

  @Test
  public void testUserAndGroupACLs() throws IOException {
    DiscoveryServiceClient discoveryService = new InMemoryDiscoveryService();
    DatasetDefinitionRegistryFactory datasetDefinitionRegistry = new DatasetDefinitionRegistryFactory() {
      @Override
      public DatasetDefinitionRegistry create() {
        return new InMemoryDatasetDefinitionRegistry();
      }
    };
    Map<String, DatasetModule> defaultModules = ImmutableMap.of(
      "orderedTable-memory", new InMemoryOrderedTableModule(),
      "metricsTable-memory", new InMemoryMetricsTableModule(),
      "core", new CoreDatasetsModule(),
      "acl", new ACLTableModule());
    DatasetFramework datasetFramework = new InMemoryDatasetFramework(datasetDefinitionRegistry, defaultModules,
                                                                     CConfiguration.create());

    ACLService aclService = new ACLService(datasetFramework, (DiscoveryService) discoveryService, discoveryService);
    aclService.startAndWait();

    ACLClient aclClient = new ACLClient(discoveryService);


    EntityId bobStream = new EntityId(EntityType.STREAM, "bob");
    Principal bobUser = new Principal(PrincipalType.USER, "bob");
    Principal bobGroup = new Principal(PrincipalType.GROUP, "bob");

    List<ACL> bobStreamAcls = aclClient.listAcls(bobStream);
    Assert.assertEquals(0, bobStreamAcls.size());

    // test user-based ACLs

    aclClient.setAclForUser(bobStream, bobUser.getId(), ImmutableList.of(PermissionType.READ, PermissionType.WRITE));

    List<ACL> bobStreamAndUserAcls = aclClient.listAcls(bobStream, bobUser.getId());
    Assert.assertEquals(1, bobStreamAndUserAcls.size());
    Assert.assertTrue(bobStreamAndUserAcls.get(0).getPermissions().contains(PermissionType.READ));
    Assert.assertTrue(bobStreamAndUserAcls.get(0).getPermissions().contains(PermissionType.WRITE));

    bobStreamAcls = aclClient.listAcls(bobStream);
    Assert.assertEquals(1, bobStreamAcls.size());
    Assert.assertTrue(bobStreamAcls.get(0).getPermissions().contains(PermissionType.READ));
    Assert.assertTrue(bobStreamAcls.get(0).getPermissions().contains(PermissionType.WRITE));

    // test group-based ACLs

    aclClient.setAclForGroup(bobStream, bobGroup.getId(), ImmutableList.of(PermissionType.READ));

    bobStreamAcls = aclClient.listAcls(bobStream);
    Assert.assertEquals(2, bobStreamAcls.size());

    int bobGroupAclIndex;
    if (bobGroup.equals(bobStreamAcls.get(0).getPrincipal())) {
      bobGroupAclIndex = 0;
    } else {
      bobGroupAclIndex = 1;
    }
    Assert.assertEquals(1, bobStreamAcls.get(bobGroupAclIndex).getPermissions().size());
    Assert.assertTrue(bobStreamAcls.get(bobGroupAclIndex).getPermissions().contains(PermissionType.READ));
  }

}
