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

package co.cask.common.authorization.client;

import co.cask.common.authorization.ACLEntry;
import co.cask.common.authorization.ACLStore;
import co.cask.common.authorization.IdentifiableObject;
import co.cask.common.authorization.InMemoryACLStore;
import co.cask.common.authorization.ObjectId;
import co.cask.common.authorization.Permission;
import co.cask.common.authorization.SubjectId;
import co.cask.common.authorization.TestObjectIds;
import co.cask.common.authorization.TestSubjectIds;
import co.cask.common.authorization.UnauthorizedException;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Test for {@link AuthorizationClient}.
 */
public class AuthorizationClientTest {

  private static final ACLEntry UNRELATED_ACL = new ACLEntry(
    TestObjectIds.application("unrelatedNamespace", "unrelatedApp"),
    TestSubjectIds.user("unrelatedUser"),
    Permission.READ
  );


  private static final ACLEntry UNRELATED_ACL2 = new ACLEntry(
    TestObjectIds.application("unrelatedNamespace", "unrelatedApp2"),
    TestSubjectIds.user("unrelatedUser"),
    Permission.READ
  );

  private AuthorizationClient authorizationClient;
  private ACLStore aclStore;

  @Before
  public void setUp() throws Exception {
    this.aclStore = new InMemoryACLStore();
    this.authorizationClient = new DefaultAuthorizationClient(aclStore);
    aclStore.write(UNRELATED_ACL);
    aclStore.write(UNRELATED_ACL2);
  }

  @Test
  public void testAuthorized() throws Exception {
    SubjectId currentUser = TestSubjectIds.user("bob");
    String namespaceId = "someNamespace";
    ObjectId objectId = TestObjectIds.application(namespaceId, "someApp");
    Permission permission = Permission.WRITE;

    aclStore.write(new ACLEntry(objectId, currentUser, permission));
    authorizationClient.authorize(objectId, ImmutableSet.of(currentUser), ImmutableSet.of(permission));
  }

  @Test
  public void testUnauthorizedNoACLEntry() throws Exception {
    SubjectId currentUser = TestSubjectIds.user("bob");
    String namespaceId = "someNamespace";
    ObjectId objectId = TestObjectIds.application(namespaceId, "someApp");
    Permission permission = Permission.WRITE;

    try {
      authorizationClient.authorize(objectId, ImmutableSet.of(currentUser), ImmutableSet.of(permission));
      Assert.fail();
    } catch (UnauthorizedException e) {
      Assert.assertTrue(e.getMessage() + " should contain namespaceId: " + namespaceId,
                        e.getMessage().contains(namespaceId));
      Assert.assertTrue(e.getMessage() + " should contain objectId: " + objectId.toString(),
                        e.getMessage().contains(objectId.toString()));
      Assert.assertTrue(e.getMessage() + " should contain permission: " + permission.toString(),
                        e.getMessage().contains(permission.toString()));
    }
  }

  @Test
  public void testUnauthorizedWrongPermission() throws Exception {
    SubjectId currentUser = TestSubjectIds.user("bob");
    String namespaceId = "someNamespace";
    ObjectId objectId = TestObjectIds.application(namespaceId, "someApp");
    Permission permission = Permission.WRITE;

    Permission wrongPermission = Permission.ADMIN;
    Assert.assertNotEquals(wrongPermission, permission);
    aclStore.write(new ACLEntry(objectId, currentUser, wrongPermission));

    try {
      authorizationClient.authorize(objectId, ImmutableSet.of(currentUser), ImmutableSet.of(permission));
      Assert.fail();
    } catch (UnauthorizedException e) {
      Assert.assertTrue(e.getMessage() + " should contain namespaceId: " + namespaceId,
                        e.getMessage().contains(namespaceId));
      Assert.assertTrue(e.getMessage() + " should contain objectId: " + objectId.toString(),
                        e.getMessage().contains(objectId.toString()));
      Assert.assertTrue(e.getMessage() + " should contain permission: " + permission.toString(),
                        e.getMessage().contains(permission.toString()));
    }
  }

  @Test
  public void testUnauthorizedWrongUser() throws Exception {
    SubjectId currentUser = TestSubjectIds.user("bob");
    String namespaceId = "someNamespace";
    ObjectId objectId = TestObjectIds.application(namespaceId, "someApp");
    Permission permission = Permission.WRITE;

    SubjectId wrongUser = TestSubjectIds.user("wrong");
    Assert.assertNotEquals(wrongUser, currentUser);
    aclStore.write(new ACLEntry(objectId, wrongUser, permission));

    try {
      authorizationClient.authorize(objectId, ImmutableSet.of(currentUser), ImmutableSet.of(permission));
      Assert.fail();
    } catch (UnauthorizedException e) {
      Assert.assertTrue(e.getMessage() + " should contain namespaceId: " + namespaceId,
                        e.getMessage().contains(namespaceId));
      Assert.assertTrue(e.getMessage() + " should contain objectId: " + objectId.toString(),
                        e.getMessage().contains(objectId.toString()));
      Assert.assertTrue(e.getMessage() + " should contain permission: " + permission.toString(),
                        e.getMessage().contains(permission.toString()));
    }
  }

  @Test
  public void testUnauthorizedWrongObject() throws Exception {
    SubjectId currentUser = TestSubjectIds.user("bob");
    String namespaceId = "someNamespace";
    ObjectId objectId = TestObjectIds.application(namespaceId, "someApp");
    Permission permission = Permission.WRITE;

    ObjectId wrongObject = TestObjectIds.application(namespaceId, "wrong");
    Assert.assertNotEquals(wrongObject, objectId);
    aclStore.write(new ACLEntry(wrongObject, currentUser, permission));

    try {
      authorizationClient.authorize(objectId, ImmutableSet.of(currentUser), ImmutableSet.of(permission));
      Assert.fail();
    } catch (UnauthorizedException e) {
      Assert.assertTrue(e.getMessage() + " should contain namespaceId: " + namespaceId,
                        e.getMessage().contains(namespaceId));
      Assert.assertTrue(e.getMessage() + " should contain objectId: " + objectId.toString(),
                        e.getMessage().contains(objectId.toString()));
      Assert.assertTrue(e.getMessage() + " should contain permission: " + permission.toString(),
                        e.getMessage().contains(permission.toString()));
    }
  }

  @Test
  public void testFilter() throws Exception {
    SubjectId currentUser = TestSubjectIds.user("bob");
    String namespaceId = "someNamespace";
    Permission permission = Permission.WRITE;

    TestApp someApp = new TestApp(namespaceId, "someApp");
    TestApp secretApp = new TestApp(namespaceId, "secretApp");
    List<TestApp> objects = ImmutableList.of(someApp, secretApp);

    aclStore.write(new ACLEntry(someApp.getObjectId(), currentUser, permission));
    List<TestApp> filtered = ImmutableList.copyOf(
      authorizationClient.filter(objects, ImmutableSet.of(currentUser), ImmutableSet.of(permission)));
    Assert.assertEquals(1, filtered.size());
    Assert.assertEquals(someApp, filtered.get(0));
  }

  @Test
  public void testHierarchy() throws Exception {
    ObjectId mainNamespace = TestObjectIds.namespace("main");
    ObjectId unusedNamespace = TestObjectIds.namespace("unused");
    ObjectId app = TestObjectIds.application(mainNamespace.getId(), "someApp");

    SubjectId user = TestSubjectIds.user("bob");
    Permission permission = Permission.WRITE;

    Assert.assertFalse(authorizationClient.isAuthorized(app, ImmutableList.of(user), ImmutableList.of(permission)));

    // unusedNamespace ACL shouldn't give permission to objects under mainNamespace
    aclStore.write(new ACLEntry(unusedNamespace, user, permission));
    Assert.assertFalse(authorizationClient.isAuthorized(app, ImmutableList.of(user), ImmutableList.of(permission)));

    // mainNamespace ACL should grant access to objects under mainNamespace
    aclStore.write(new ACLEntry(mainNamespace, user, permission));
    Assert.assertTrue(authorizationClient.isAuthorized(app, ImmutableList.of(user), ImmutableList.of(permission)));

    // deleting mainNamespace ACL should deny access to objects under mainNamespace without other ACLs
    aclStore.delete(new ACLEntry(mainNamespace, user, permission));
    Assert.assertFalse(authorizationClient.isAuthorized(app, ImmutableList.of(user), ImmutableList.of(permission)));
  }

  /**
   *
   */
  private static final class TestApp implements IdentifiableObject {

    private final String id;
    private final String namespaceId;

    private TestApp(String namespaceId, String id) {
      this.namespaceId = namespaceId;
      this.id = id;
    }

    @Override
    public ObjectId getObjectId() {
      return TestObjectIds.application(namespaceId, id);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(id, namespaceId);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final TestApp other = (TestApp) obj;
      return Objects.equal(this.id, other.id) && Objects.equal(this.namespaceId, other.namespaceId);
    }
  }
}
