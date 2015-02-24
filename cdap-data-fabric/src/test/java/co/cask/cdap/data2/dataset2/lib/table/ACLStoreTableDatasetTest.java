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
import co.cask.cdap.common.authorization.ObjectIds;
import co.cask.cdap.common.authorization.SubjectIds;
import co.cask.cdap.data2.dataset2.AbstractDatasetTest;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import co.cask.cdap.proto.Id;
import co.cask.common.authorization.ACLEntry;
import co.cask.common.authorization.ACLStore;
import co.cask.common.authorization.ObjectId;
import co.cask.common.authorization.Permission;
import co.cask.common.authorization.SubjectId;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;

/**
 * Test for {@link ACLStoreTableDataset}.
 */
public class ACLStoreTableDatasetTest extends AbstractDatasetTest {

  private static final ACLEntry NAMESPACE_ACL = new ACLEntry(
    ObjectIds.namespace("testNamespace"),
    SubjectIds.user("bob"),
    Permission.LIFECYCLE
  );

  private static final ACLEntry SIMPLE_ACL = new ACLEntry(
    ObjectIds.application("someNamespace", "someApp"),
    SubjectIds.user("bob"),
    Permission.WRITE
  );

  private static final ACLEntry SIMPLE_ACL_PARENT = new ACLEntry(
    ObjectIds.namespace("someNamespace"),
    SubjectIds.user("bob"),
    Permission.WRITE
  );

  private static final ACLEntry OTHER_ACL = new ACLEntry(
    ObjectIds.application("otherNamespace", "otherApp"),
    SubjectIds.user("otherUser"),
    Permission.READ
  );

  private static final ACLEntry OTHER_ACL2 = new ACLEntry(
    ObjectIds.application("otherNamespace", "otherApp"),
    SubjectIds.user("otherUser"),
    Permission.LIFECYCLE
  );

  private static final ACLEntry UNRELATED_ACL = new ACLEntry(
    ObjectIds.application("unrelatedNamespace", "unrelatedApp"),
    SubjectIds.user("unrelatedUser"),
    Permission.READ
  );


  private static final ACLEntry UNRELATED_ACL2 = new ACLEntry(
    ObjectIds.application("unrelatedNamespace", "unrelatedApp2"),
    SubjectIds.user("unrelatedUser"),
    Permission.READ
  );

  private static final Id.DatasetModule TABLE_MODULE_ID = Id.DatasetModule.from(
    Id.Namespace.from("default"), InMemoryTableModule.class.getName());

  private static final Id.DatasetModule CORE_MODULE_ID = Id.DatasetModule.from(
    Id.Namespace.from("default"), CoreDatasetsModule.class.getName());

  private static final Id.DatasetModule MODULE_ID = Id.DatasetModule.from(
    Id.Namespace.from("default"), ACLStoreTableModule.class.getName());

  private static final Id.DatasetInstance DATASET_ID = Id.DatasetInstance.from(
    Id.Namespace.from("default"), "testACLStoreTable");

  private ACLStoreTable aclStore;

  @Before
  public void setUp() throws Exception {
    addModule(TABLE_MODULE_ID, new InMemoryTableModule());
    addModule(CORE_MODULE_ID, new CoreDatasetsModule());
    addModule(MODULE_ID, new ACLStoreTableModule());
    createInstance(ACLStoreTable.class.getName(), DATASET_ID, DatasetProperties.EMPTY);
    this.aclStore = getInstance(DATASET_ID);
    aclStore.write(UNRELATED_ACL);
    aclStore.write(UNRELATED_ACL2);
  }

  @After
  public void tearDown() throws Exception {
    deleteInstance(DATASET_ID);
    deleteModule(MODULE_ID);
    deleteModule(CORE_MODULE_ID);
    deleteModule(TABLE_MODULE_ID);
  }

  @Test
  public void testWriteAndDelete() throws Exception {
    Assert.assertFalse(aclStore.exists(SIMPLE_ACL));
    aclStore.write(SIMPLE_ACL);
    Assert.assertTrue(aclStore.exists(SIMPLE_ACL));
    aclStore.delete(SIMPLE_ACL);
    Assert.assertFalse(aclStore.exists(SIMPLE_ACL));
  }

  @Test
  public void testSearchAndDeleteOther() throws Exception {
    testSearchAndDelete(ImmutableList.of(OTHER_ACL, OTHER_ACL2),
                        ImmutableList.of(OTHER_ACL, OTHER_ACL2),
                        ImmutableList.of(new ACLStore.Query(OTHER_ACL.getObject(), OTHER_ACL.getSubject())),
                        ImmutableList.<ACLEntry>of());
  }

  @Test
  public void testSearchAndDeleteNamespaceBySimpleQuery() throws Exception {
    testSearchAndDelete(ImmutableList.of(NAMESPACE_ACL),
                        ImmutableList.of(NAMESPACE_ACL),
                        ImmutableList.of(new ACLStore.Query(NAMESPACE_ACL)),
                        ImmutableList.<ACLEntry>of());
  }

  @Test
  public void testSearchAndDeleteNamespaceByObject() throws Exception {
    testSearchAndDelete(ImmutableList.of(NAMESPACE_ACL),
                        ImmutableList.of(NAMESPACE_ACL),
                        ImmutableList.of(new ACLStore.Query(NAMESPACE_ACL.getObject(), null, null)),
                        ImmutableList.<ACLEntry>of());
  }

  @Test
  public void testSearchAndDeleteBySimpleQuery() throws Exception {
    aclStore.write(SIMPLE_ACL_PARENT);
    testSearchAndDelete(ImmutableList.of(SIMPLE_ACL),
                        ImmutableList.of(SIMPLE_ACL),
                        ImmutableList.of(new ACLStore.Query(SIMPLE_ACL)),
                        ImmutableList.of(SIMPLE_ACL_PARENT));
  }

  @Test
  public void testSearchAndDeleteByObject() throws Exception {
    aclStore.write(SIMPLE_ACL_PARENT);
    testSearchAndDelete(ImmutableList.of(SIMPLE_ACL),
                        ImmutableList.of(SIMPLE_ACL),
                        ImmutableList.of(new ACLStore.Query(SIMPLE_ACL.getObject(), null, null)),
                        ImmutableList.of(SIMPLE_ACL_PARENT));
  }

  @Test
  public void testSearchAndDeleteBySubject() throws Exception {
    aclStore.write(SIMPLE_ACL_PARENT);
    testSearchAndDelete(ImmutableList.of(SIMPLE_ACL),
                        ImmutableList.of(SIMPLE_ACL, SIMPLE_ACL_PARENT),
                        ImmutableList.of(new ACLStore.Query(null, SIMPLE_ACL.getSubject(), null)),
                        ImmutableList.<ACLEntry>of());
  }

  @Test
  public void testSearchAndDeleteByPermission() throws Exception {
    aclStore.write(SIMPLE_ACL_PARENT);
    testSearchAndDelete(ImmutableList.of(SIMPLE_ACL),
                        ImmutableList.of(SIMPLE_ACL, SIMPLE_ACL_PARENT),
                        ImmutableList.of(new ACLStore.Query(null, null, SIMPLE_ACL.getPermission())),
                        ImmutableList.<ACLEntry>of());
  }

  @Test
  public void testMultiSearchAndDelete() throws Exception {
    aclStore.write(SIMPLE_ACL_PARENT);

    ACLEntry sameObjectAndSubject = new ACLEntry(SIMPLE_ACL.getObject(), SIMPLE_ACL.getSubject(),
                                                 Permission.LIFECYCLE);
    Assert.assertNotEquals(sameObjectAndSubject.getPermission(), SIMPLE_ACL.getPermission());

    testSearchAndDelete(ImmutableList.of(SIMPLE_ACL, sameObjectAndSubject),
                        ImmutableList.of(SIMPLE_ACL, sameObjectAndSubject),
                        ImmutableList.of(new ACLStore.Query(SIMPLE_ACL.getObject(), SIMPLE_ACL.getSubject(), null)),
                        ImmutableList.<ACLEntry>of());
  }

  protected void testSearchAndDelete(List<ACLEntry> entriesToWrite,
                                     List<ACLEntry> expectedEntriesBeforeDelete,
                                     Iterable<ACLStore.Query> deleteQuery,
                                     List<ACLEntry> expectedEntriesAfterDelete) throws Exception {
    // create and check all entries exist
    for (ACLEntry entry : entriesToWrite) {
      aclStore.write(entry);
      Assert.assertTrue(aclStore.exists(entry));
    }

    // search and delete all entries
    Set<ACLEntry> searchResults = aclStore.search(deleteQuery);
    for (ACLEntry entry : expectedEntriesBeforeDelete) {
      aclStore.write(entry);
      Assert.assertTrue(searchResults.contains(entry));
    }
    Assert.assertEquals(expectedEntriesBeforeDelete.size(), searchResults.size());
    aclStore.delete(deleteQuery);

    // ensure all entries deleted
    for (ACLEntry entry : entriesToWrite) {
      for (ACLStore.Query query : deleteQuery) {
        if (query.matches(entry)) {
          Assert.assertFalse(aclStore.exists(entry));
        }
      }
    }

    for (ACLEntry entry : expectedEntriesAfterDelete) {
      Assert.assertTrue("Expected '" + entry + "' to exist", aclStore.exists(entry));
    }
  }

  @Test
  public void testAnyPermission() throws Exception {
    // asking for ANY permission gives ACL with ANY permission if any ACLs exist
    // for the (object, subject) pair for any permissions
    SubjectId currentUser = SubjectIds.user("bob");
    String namespaceId = "someNamespace";
    ObjectId objectId = ObjectIds.application(namespaceId, "someApp");

    aclStore.write(new ACLEntry(objectId, currentUser, Permission.READ));

    Assert.assertTrue(aclStore.exists(new ACLEntry(objectId, currentUser, Permission.ANY)));
    Set<ACLEntry> searchResults = aclStore.search(new ACLStore.Query(objectId, currentUser, Permission.ANY));
    Assert.assertEquals(1, searchResults.size());
    Assert.assertTrue(searchResults.contains(new ACLEntry(objectId, currentUser, Permission.ANY)));
  }

  @Test
  public void testAdminPermission() throws Exception {
    SubjectId currentUser = SubjectIds.user("bob");
    String namespaceId = "someNamespace";
    ObjectId objectId = ObjectIds.application(namespaceId, "someApp");

    aclStore.write(new ACLEntry(objectId, currentUser, Permission.ADMIN));

    Assert.assertTrue(aclStore.exists(new ACLEntry(objectId, currentUser, Permission.READ)));
    Set<ACLEntry> searchResults = aclStore.search(new ACLStore.Query(objectId, currentUser, Permission.READ));
    Assert.assertEquals(1, searchResults.size());
    Assert.assertEquals(new ACLEntry(objectId, currentUser, Permission.READ), searchResults.iterator().next());
  }
}
