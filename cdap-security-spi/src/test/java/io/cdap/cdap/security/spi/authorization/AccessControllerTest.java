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
package io.cdap.cdap.security.spi.authorization;

import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.GrantedPermission;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Role;
import io.cdap.cdap.proto.security.StandardPermission;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

/**
 * Tests for {@link AccessController}.
 */
public abstract class AccessControllerTest {

  private final NamespaceId namespace = new NamespaceId("foo");
  private final Principal user = new Principal("alice", Principal.PrincipalType.USER);

  protected abstract AccessController get();

  @Test
  public void testSimple() throws Exception {
    AccessController authorizer = get();

    verifyAuthFailure(namespace, user, StandardPermission.GET);

    authorizer.grant(Authorizable.fromEntityId(namespace), user, Collections.singleton(StandardPermission.GET));
    authorizer.enforce(namespace, user, StandardPermission.GET);

    Set<GrantedPermission> expectedPrivileges = new HashSet<>();
    expectedPrivileges.add(new GrantedPermission(namespace, StandardPermission.GET));
    Assert.assertEquals(expectedPrivileges, authorizer.listGrants(user));

    authorizer.revoke(Authorizable.fromEntityId(namespace), user, Collections.singleton(StandardPermission.GET));
    verifyAuthFailure(namespace, user, StandardPermission.GET);
  }

  @Test
  public void testWildcard() throws Exception {
    AccessController authorizer = get();

    verifyAuthFailure(namespace, user, StandardPermission.GET);

    authorizer.grant(Authorizable.fromEntityId(namespace), user, EnumSet.allOf(StandardPermission.class));
    authorizer.enforce(namespace, user, StandardPermission.GET);
    authorizer.enforce(namespace, user, StandardPermission.UPDATE);

    authorizer.revoke(Authorizable.fromEntityId(namespace), user, EnumSet.allOf(StandardPermission.class));
    verifyAuthFailure(namespace, user, StandardPermission.GET);
  }

  @Test
  public void testAll() throws Exception {
    AccessController authorizer = get();

    verifyAuthFailure(namespace, user, StandardPermission.GET);

    authorizer.grant(Authorizable.fromEntityId(namespace), user, EnumSet.allOf(StandardPermission.class));
    authorizer.enforce(namespace, user, StandardPermission.GET);
    authorizer.enforce(namespace, user, StandardPermission.UPDATE);

    authorizer.revoke(Authorizable.fromEntityId(namespace), user, EnumSet.allOf(StandardPermission.class));
    verifyAuthFailure(namespace, user, StandardPermission.GET);

    Principal role = new Principal("admins", Principal.PrincipalType.ROLE);
    authorizer.grant(Authorizable.fromEntityId(namespace), user, Collections.singleton(StandardPermission.GET));
    authorizer.grant(Authorizable.fromEntityId(namespace), role, EnumSet.allOf(StandardPermission.class));
    authorizer.revoke(Authorizable.fromEntityId(namespace));
    verifyAuthFailure(namespace, user, StandardPermission.GET);
    verifyAuthFailure(namespace, role, StandardPermission.GET);
    verifyAuthFailure(namespace, role, StandardPermission.UPDATE);
  }

  @Test
  public void testRBAC() throws Exception {
    AccessController authorizer = get();

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
    } catch (AlreadyExistsException expected) {
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
    } catch (NotFoundException expected) {
      // expected
    }

    // add an user to an existing role
    Principal spiderman = new Principal("spiderman", Principal.PrincipalType.USER);
    authorizer.addRoleToPrincipal(engineers, spiderman);

    // add an user to an non-existing role should throw an exception
    try {
      authorizer.addRoleToPrincipal(admins, spiderman);
      Assert.fail(String.format("Added role %s to principal %s. Should have failed.", admins, spiderman));
    } catch (NotFoundException expected) {
      // expectedRoles
    }

    // check listing roles for spiderman have engineers role
    Assert.assertEquals(Collections.singleton(engineers), authorizer.listRoles(spiderman));

    // authorization checks with roles
    NamespaceId ns1 = new NamespaceId("ns1");

    // check that spiderman who has engineers roles cannot read from ns1
    verifyAuthFailure(ns1, spiderman, StandardPermission.GET);

    // give a permission to engineers role
    authorizer.grant(Authorizable.fromEntityId(ns1), engineers, Collections.singleton(StandardPermission.GET));

    // check that a spiderman who has engineers role has access
    authorizer.enforce(ns1, spiderman, StandardPermission.GET);

    // list privileges for spiderman should have read permission on ns1
    Assert.assertEquals(Collections.singleton(new GrantedPermission(ns1, StandardPermission.GET)),
                        authorizer.listGrants(spiderman));

    // revoke permission from the role
    authorizer.revoke(Authorizable.fromEntityId(ns1), engineers, Collections.singleton(StandardPermission.GET));

    // now the privileges for spiderman should be empty
    Assert.assertEquals(Collections.EMPTY_SET, authorizer.listGrants(spiderman));

    // check that the user of this role is not authorized to do the revoked operation
    verifyAuthFailure(ns1, spiderman, StandardPermission.GET);

    // remove an user from a existing role
    authorizer.removeRoleFromPrincipal(engineers, spiderman);

    // check listing roles for spiderman should be empty
    Assert.assertEquals(Collections.EMPTY_SET, authorizer.listRoles(spiderman));

    // remove an user from a non-existing role should throw exception
    try {
      authorizer.removeRoleFromPrincipal(admins, spiderman);
      Assert.fail(String.format("Removed non-existing role %s from principal %s. Should have failed.", admins,
                                spiderman));
    } catch (NotFoundException expected) {
      // expectedRoles
    }
  }

  private void verifyAuthFailure(EntityId entity, Principal principal, Permission permission) throws Exception {
    try {
      get().enforce(entity, principal, permission);
      Assert.fail(String.format("Expected authorization failure, but it succeeded for entity %s, principal %s," +
                                  " permission %s", entity, principal, permission));
    } catch (UnauthorizedException expected) {
      // expected
    }
  }
}
