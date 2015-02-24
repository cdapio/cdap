/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.authorization;

import co.cask.cdap.common.authorization.ObjectIds;
import co.cask.cdap.common.authorization.SubjectIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.common.authorization.ACLEntry;
import co.cask.common.authorization.ACLStore;
import co.cask.common.authorization.ObjectId;
import co.cask.common.authorization.Permission;
import co.cask.common.authorization.SubjectId;
import co.cask.common.authorization.client.NoopAuthorizationClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

/**
 * Test for {@link ACLManagerService} and {@link ACLManagerClient}.
 */
public class ACLManagerServiceTest {

  private ACLManagerService aclManagerService;
  private ACLManagerClient aclManagerClient;

  @Before
  public void setUp() throws Exception {
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    aclManagerService = new ACLManagerService(CConfiguration.create(), discoveryService,
                                              new InMemoryACLStoreTableSupplier(), new NoopAuthorizationClient());
    aclManagerClient = new ACLManagerClient(new DiscoveringBaseURISupplier(discoveryService));
    aclManagerService.startAndWait();
  }

  @After
  public void tearDown() {
    aclManagerService.stopAndWait();
  }

  @Test
  public void testSet() throws IOException {
    SubjectId currentUser = SubjectIds.user("bob");
    String namespaceId = "someNamespace";
    ObjectId objectId = ObjectIds.application(namespaceId, "someApp");
    Permission permission = Permission.DELETE;

    Set<ACLEntry> beforeAcls = aclManagerClient.searchACLs(new ACLStore.Query(objectId, currentUser));
    Assert.assertEquals(0, beforeAcls.size());

    ACLEntry entry = new ACLEntry(objectId, currentUser, permission);
    aclManagerClient.createACL(entry);

    Set<ACLEntry> afterAcls = aclManagerClient.searchACLs(new ACLStore.Query(objectId, currentUser));
    Assert.assertEquals(1, afterAcls.size());
    ACLEntry afterAcl = afterAcls.iterator().next();
    Assert.assertEquals(currentUser, afterAcl.getSubject());
    Assert.assertEquals(objectId, afterAcl.getObject());
    Assert.assertEquals(permission, afterAcl.getPermission());
  }

  @Test
  public void testDelete() throws IOException {
    SubjectId currentUser = SubjectIds.user("bob");
    String namespaceId = "someNamespace";
    ObjectId objectId = ObjectIds.application(namespaceId, "someApp");
    Permission permission = Permission.DELETE;

    ACLEntry entry = new ACLEntry(objectId, currentUser, permission);
    aclManagerClient.createACL(entry);

    Set<ACLEntry> beforeAcls = aclManagerClient.searchACLs(new ACLStore.Query(objectId, currentUser));
    Assert.assertEquals(1, beforeAcls.size());

    aclManagerClient.deleteACLs(new ACLStore.Query(entry));

    Set<ACLEntry> afterAcls = aclManagerClient.searchACLs(new ACLStore.Query(objectId, currentUser));
    Assert.assertEquals(0, afterAcls.size());
  }

  @Test
  public void testNamespace() throws Exception {
    SubjectId currentUser = SubjectIds.user("bob");
    String namespaceId = "someNamespace";
    String otherNamespaceId = "otherNamespace";

    Set<ACLEntry> entries = aclManagerClient.searchACLs(new ACLStore.Query(null, null, null));
    Assert.assertEquals(0, entries.size());
  }
}
