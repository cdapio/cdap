/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
package co.cask.cdap.security.spi.authorization;

import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.proto.security.Role;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

/**
 * Tests for {@link Authorizer}.
 */
public abstract class AuthorizerTest {

  private final NamespaceId namespace = new NamespaceId("foo");
  private final Principal user = new Principal("alice", Principal.PrincipalType.USER);

  protected abstract Authorizer get();

  @Test
  public void testSimple() throws Exception {
    Authorizer authorizer = get();

    verifyAuthFailure(namespace, user, Action.READ);

    authorizer.grant(namespace, user, Collections.singleton(Action.READ));
    authorizer.enforce(namespace, user, Action.READ);

    Set<Privilege> expectedPrivileges = new HashSet<>();
    expectedPrivileges.add(new Privilege(namespace, Action.READ));
    Assert.assertEquals(expectedPrivileges, authorizer.listPrivileges(user));

    authorizer.revoke(namespace, user, Collections.singleton(Action.READ));
    verifyAuthFailure(namespace, user, Action.READ);
  }

  @Test
  public void testWildcard() throws Exception {
    Authorizer authorizer = get();

    verifyAuthFailure(namespace, user, Action.READ);

    authorizer.grant(namespace, user, EnumSet.allOf(Action.class));
    authorizer.enforce(namespace, user, Action.READ);
    authorizer.enforce(namespace, user, Action.WRITE);
    authorizer.enforce(namespace, user, Action.ADMIN);
    authorizer.enforce(namespace, user, Action.EXECUTE);

    authorizer.revoke(namespace, user, EnumSet.allOf(Action.class));
    verifyAuthFailure(namespace, user, Action.READ);
  }

  @Test
  public void testAll() throws Exception {
    Authorizer authorizer = get();

    verifyAuthFailure(namespace, user, Action.READ);

    authorizer.grant(namespace, user, EnumSet.allOf(Action.class));
    authorizer.enforce(namespace, user, Action.READ);
    authorizer.enforce(namespace, user, Action.WRITE);
    authorizer.enforce(namespace, user, Action.ADMIN);
    authorizer.enforce(namespace, user, Action.EXECUTE);

    authorizer.revoke(namespace, user, EnumSet.allOf(Action.class));
    verifyAuthFailure(namespace, user, Action.READ);

    Principal role = new Principal("admins", Principal.PrincipalType.ROLE);
    authorizer.grant(namespace, user, Collections.singleton(Action.READ));
    authorizer.grant(namespace, role, EnumSet.allOf(Action.class));
    authorizer.revoke(namespace);
    verifyAuthFailure(namespace, user, Action.READ);
    verifyAuthFailure(namespace, role, Action.ADMIN);
    verifyAuthFailure(namespace, role, Action.READ);
    verifyAuthFailure(namespace, role, Action.WRITE);
    verifyAuthFailure(namespace, role, Action.EXECUTE);
  }

  @Test
  // TODO: Enable when hierarchy is supported
  @Ignore
  public void testHierarchy() throws Exception {
    Authorizer authorizer = get();

    DatasetId dataset = namespace.dataset("bar");

    verifyAuthFailure(namespace, user, Action.READ);

    authorizer.grant(namespace, user, Collections.singleton(Action.READ));
    authorizer.enforce(dataset, user, Action.READ);

    authorizer.grant(dataset, user, Collections.singleton(Action.WRITE));
    verifyAuthFailure(namespace, user, Action.WRITE);

    authorizer.revoke(namespace, user, Collections.singleton(Action.READ));
    authorizer.revoke(dataset);
    verifyAuthFailure(namespace, user, Action.READ);
  }

  @Test
  public void testRBAC() throws Exception {
    Authorizer authorizer = get();

    Role admins = new Role("admins");
    Role engineers = new Role("engineers");
    // create a role
    authorizer.createRole(admins);
    // add another role
    authorizer.createRole(engineers);

    // listing role should show the added role
    Set<Role> roles = authorizer.listAllRoles();
    Set<Role> expectedRoles = new HashSet<>();
    expectedRoles.add(admins);
    expectedRoles.add(engineers);
    Assert.assertEquals(expectedRoles, roles);

    // creating a role which already exists should throw an exception
    try {
      authorizer.createRole(admins);
      Assert.fail(String.format("Created a role %s which already exists. Should have failed.", admins.getName()));
    } catch (RoleAlreadyExistsException expected) {
      // expected
    }

    // drop an existing role
    authorizer.dropRole(admins);

    // the list should not have the dropped role
    roles = authorizer.listAllRoles();
    Assert.assertEquals(Collections.singleton(engineers), roles);

    // dropping a non-existing role should throw exception
    try {
      authorizer.dropRole(admins);
      Assert.fail(String.format("Dropped a role %s which does not exists. Should have failed.", admins.getName()));
    } catch (RoleNotFoundException expected) {
      // expected
    }

    // add an user to an existing role
    Principal spiderman = new Principal("spiderman", Principal.PrincipalType.USER);
    authorizer.addRoleToPrincipal(engineers, spiderman);

    // add an user to an non-existing role should throw an exception
    try {
      authorizer.addRoleToPrincipal(admins, spiderman);
      Assert.fail(String.format("Added role %s to principal %s. Should have failed.", admins, spiderman));
    } catch (RoleNotFoundException expected) {
      // expectedRoles
    }

    // check listing roles for spiderman have engineers role
    Assert.assertEquals(Collections.singleton(engineers), authorizer.listRoles(spiderman));

    // authorization checks with roles
    NamespaceId ns1 = new NamespaceId("ns1");

    // check that spiderman who has engineers roles cannot read from ns1
    verifyAuthFailure(ns1, spiderman, Action.READ);

    // give a permission to engineers role
    authorizer.grant(ns1, engineers, Collections.singleton(Action.READ));

    // check that a spiderman who has engineers role has access
    authorizer.enforce(ns1, spiderman, Action.READ);

    // list privileges for spiderman should have read action on ns1
    Assert.assertEquals(Collections.singleton(new Privilege(ns1, Action.READ)),
                        authorizer.listPrivileges(spiderman));

    // revoke action from the role
    authorizer.revoke(ns1, engineers, Collections.singleton(Action.READ));

    // now the privileges for spiderman should be empty
    Assert.assertEquals(Collections.EMPTY_SET, authorizer.listPrivileges(spiderman));

    // check that the user of this role is not authorized to do the revoked operation
    verifyAuthFailure(ns1, spiderman, Action.READ);

    // remove an user from a existing role
    authorizer.removeRoleFromPrincipal(engineers, spiderman);

    // check listing roles for spiderman should be empty
    Assert.assertEquals(Collections.EMPTY_SET, authorizer.listRoles(spiderman));

    // remove an user from a non-existing role should throw exception
    try {
      authorizer.removeRoleFromPrincipal(admins, spiderman);
      Assert.fail(String.format("Removed non-existing role %s from principal %s. Should have failed.", admins,
                                spiderman));
    } catch (RoleNotFoundException expected) {
      // expectedRoles
    }
  }

  private void verifyAuthFailure(EntityId entity, Principal principal, Action action) throws Exception {
    try {
      get().enforce(entity, principal, action);
      Assert.fail(String.format("Expected authorization failure, but it succeeded for entity %s, principal %s," +
                                  " action %s", entity, principal, action));
    } catch (UnauthorizedException expected) {
      // expected
    }
  }
}
