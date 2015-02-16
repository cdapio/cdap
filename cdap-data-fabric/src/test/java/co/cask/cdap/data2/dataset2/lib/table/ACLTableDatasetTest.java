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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.ACLTable;
import co.cask.cdap.api.security.ACL;
import co.cask.cdap.api.security.EntityId;
import co.cask.cdap.api.security.EntityType;
import co.cask.cdap.api.security.PermissionType;
import co.cask.cdap.api.security.Principal;
import co.cask.cdap.api.security.PrincipalType;
import co.cask.cdap.data2.dataset2.AbstractDatasetTest;
import co.cask.cdap.proto.Id;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Test for {@link co.cask.cdap.data2.dataset2.lib.table.ACLTableDataset}.
 */
public class ACLTableDatasetTest extends AbstractDatasetTest {

  private static final Id.DatasetModule aclTableModule = Id.DatasetModule.from(NAMESPACE_ID, "aclTableModule");

  @Test
  public void testBasics() throws Exception {
    addModule(aclTableModule, new ACLTableModule());

    createInstance(ACLTable.class.getName(), "myAclTable", DatasetProperties.EMPTY);
    ACLTable myAclTable = getInstance("myAclTable");

    final Principal bobUser = new Principal(PrincipalType.USER, "bob");
    final Principal bobGroup = new Principal(PrincipalType.GROUP, "bobs-only");

    final EntityId secretEntity = new EntityId(EntityType.STREAM, "secretEntity");

    // set user ACL
    myAclTable.setAcl(bobUser, secretEntity, PermissionType.READ, PermissionType.WRITE);
    List<ACL> acls = myAclTable.getAcls(secretEntity);
    Assert.assertEquals(1, acls.size());

    Assert.assertEquals(2, acls.get(0).getPermissions().size());
    Assert.assertEquals(bobUser.getQualifiedId(), acls.get(0).getPrincipal().getQualifiedId());
    Assert.assertTrue(acls.get(0).getPermissions().contains(PermissionType.READ));
    Assert.assertTrue(acls.get(0).getPermissions().contains(PermissionType.WRITE));

    // set group ACL
    myAclTable.setAcl(bobGroup, secretEntity, PermissionType.WRITE);
    acls = myAclTable.getAcls(secretEntity);
    Assert.assertEquals(2, acls.size());

    ACL actualBobAcl;
    ACL actualBobsAcl;
    if (acls.get(0).getPrincipal().getQualifiedId().equals(bobUser.getQualifiedId())) {
      actualBobAcl = acls.get(0);
      actualBobsAcl = acls.get(1);
    } else {
      actualBobAcl = acls.get(1);
      actualBobsAcl = acls.get(0);
    }

    Assert.assertEquals(bobUser.getQualifiedId(), actualBobAcl.getPrincipal().getQualifiedId());
    Assert.assertEquals(2, actualBobAcl.getPermissions().size());
    Assert.assertTrue(actualBobAcl.getPermissions().contains(PermissionType.READ));
    Assert.assertTrue(actualBobAcl.getPermissions().contains(PermissionType.WRITE));

    Assert.assertEquals(bobGroup.getQualifiedId(), actualBobsAcl.getPrincipal().getQualifiedId());
    Assert.assertEquals(1, actualBobsAcl.getPermissions().size());
    Assert.assertTrue(actualBobsAcl.getPermissions().contains(PermissionType.WRITE));

    deleteModule(aclTableModule);
  }

}
