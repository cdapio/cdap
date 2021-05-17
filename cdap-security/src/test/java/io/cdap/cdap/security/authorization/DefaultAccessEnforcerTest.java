/*
 * Copyright © 2016-2021 Cask Data, Inc.
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

package io.cdap.cdap.security.authorization;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authorization.AccessController;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Tests for {@link DefaultAccessEnforcer}.
 */
public class DefaultAccessEnforcerTest extends AuthorizationTestBase {

  private static final Principal ALICE = new Principal("alice", Principal.PrincipalType.USER);
  private static final Principal BOB = new Principal("bob", Principal.PrincipalType.USER);
  private static final NamespaceId NS = new NamespaceId("ns");
  private static final ApplicationId APP = NS.app("app");

  @BeforeClass
  public static void setupClass() throws IOException {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, InMemoryAccessController.class.getName());
    Location externalAuthJar = AppJarHelper.createDeploymentJar(
      locationFactory, InMemoryAccessController.class, manifest);
    CCONF.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, externalAuthJar.toString());
  }

  @Test
  public void testAuthenticationDisabled() throws IOException, AccessException {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    cConfCopy.setBoolean(Constants.Security.ENABLED, false);
    verifyDisabled(cConfCopy);
  }

  @Test
  public void testAuthorizationDisabled() throws IOException, AccessException {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    cConfCopy.setBoolean(Constants.Security.Authorization.ENABLED, false);
    verifyDisabled(cConfCopy);
  }

  @Test
  public void testPropagationDisabled() throws IOException, AccessException {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    try (AccessControllerInstantiator accessControllerInstantiator =
           new AccessControllerInstantiator(cConfCopy, AUTH_CONTEXT_FACTORY)) {
      DefaultAccessEnforcer accessEnforcer =
        new DefaultAccessEnforcer(cConfCopy, accessControllerInstantiator);
      accessControllerInstantiator.get().grant(Authorizable.fromEntityId(NS), ALICE,
                                               ImmutableSet.of(StandardPermission.UPDATE));
      accessEnforcer.enforce(NS, ALICE, StandardPermission.UPDATE);
      try {
        accessEnforcer.enforce(APP, ALICE, StandardPermission.UPDATE);
        Assert.fail("Alice should not have ADMIN privilege on the APP.");
      } catch (UnauthorizedException ignored) {
        // expected
      }
    }
  }

  @Test
  public void testAuthEnforce() throws IOException, AccessException {
    try (AccessControllerInstantiator accessControllerInstantiator =
           new AccessControllerInstantiator(CCONF, AUTH_CONTEXT_FACTORY)) {
      AccessController accessController = accessControllerInstantiator.get();
      DefaultAccessEnforcer authEnforcementService =
        new DefaultAccessEnforcer(CCONF, accessControllerInstantiator);
      // update privileges for alice. Currently alice has not been granted any privileges.
      assertAuthorizationFailure(authEnforcementService, NS, ALICE, StandardPermission.UPDATE);

      // grant some test privileges
      DatasetId ds = NS.dataset("ds");
      accessController.grant(Authorizable.fromEntityId(NS), ALICE, ImmutableSet.of(StandardPermission.GET,
                                                                                   StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ds), BOB, ImmutableSet.of(StandardPermission.UPDATE));

      // auth enforcement for alice should succeed on ns for actions read and write
      authEnforcementService.enforce(NS, ALICE, ImmutableSet.of(StandardPermission.GET, StandardPermission.UPDATE));
      assertAuthorizationFailure(authEnforcementService, NS, ALICE, EnumSet.allOf(StandardPermission.class));
      // alice do not have READ or WRITE on the dataset, so authorization should fail
      assertAuthorizationFailure(authEnforcementService, ds, ALICE, StandardPermission.GET);
      assertAuthorizationFailure(authEnforcementService, ds, ALICE, StandardPermission.UPDATE);

      // Alice doesn't have Delete right on NS, hence should fail.
      assertAuthorizationFailure(authEnforcementService, NS, ALICE, StandardPermission.DELETE);
      // bob enforcement should succeed since we grant him admin privilege
      authEnforcementService.enforce(ds, BOB, StandardPermission.UPDATE);
      // revoke all of alice's privileges
      accessController.revoke(Authorizable.fromEntityId(NS), ALICE, ImmutableSet.of(StandardPermission.GET));
      try {
        authEnforcementService.enforce(NS, ALICE, StandardPermission.GET);
        Assert.fail(String.format("Expected %s to not have '%s' privilege on %s but it does.",
                                  ALICE, StandardPermission.GET, NS));
      } catch (UnauthorizedException ignored) {
        // expected
      }
      accessController.revoke(Authorizable.fromEntityId(NS));

      assertAuthorizationFailure(authEnforcementService, NS, ALICE, StandardPermission.GET);
      assertAuthorizationFailure(authEnforcementService, NS, ALICE, StandardPermission.UPDATE);
      authEnforcementService.enforce(ds, BOB, StandardPermission.UPDATE);
    }
  }

  @Test
  public void testSingleIsVisible() throws IOException, AccessException {
    try (AccessControllerInstantiator accessControllerInstantiator =
           new AccessControllerInstantiator(CCONF, AUTH_CONTEXT_FACTORY)) {
      AccessController accessController = accessControllerInstantiator.get();
      NamespaceId ns1 = new NamespaceId("ns1");
      NamespaceId ns2 = new NamespaceId("ns2");
      DatasetId ds11 = ns1.dataset("ds11");
      DatasetId ds12 = ns1.dataset("ds12");
      DatasetId ds21 = ns2.dataset("ds21");
      DatasetId ds22 = ns2.dataset("ds22");
      DatasetId ds23 = ns2.dataset("ds33");
      Set<NamespaceId> namespaces = ImmutableSet.of(ns1, ns2);
      // Alice has access on ns1, ns2, ds11, ds21, ds23, Bob has access on ds11, ds12, ds22
      accessController.grant(Authorizable.fromEntityId(ns1), ALICE, Collections.singleton(StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ns2), ALICE, Collections.singleton(StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ds11), ALICE, Collections.singleton(StandardPermission.GET));
      accessController.grant(Authorizable.fromEntityId(ds11), BOB, Collections.singleton(StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ds21), ALICE, Collections.singleton(StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ds12), BOB, Collections.singleton(StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ds12), BOB, EnumSet.allOf(StandardPermission.class));
      accessController.grant(Authorizable.fromEntityId(ds21), ALICE, Collections.singleton(StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ds23), ALICE, Collections.singleton(StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ds22), BOB, Collections.singleton(StandardPermission.UPDATE));
      DefaultAccessEnforcer authEnforcementService =
        new DefaultAccessEnforcer(CCONF, accessControllerInstantiator);
      authEnforcementService.isVisible(ns1, ALICE);
      authEnforcementService.isVisible(ns2, ALICE);
      // bob should also be able to list two namespaces since he has privileges on the dataset in both namespaces
      authEnforcementService.isVisible(ns1, BOB);
      authEnforcementService.isVisible(ns2, BOB);
      authEnforcementService.isVisible(ds11, ALICE);
      authEnforcementService.isVisible(ds21, ALICE);
      authEnforcementService.isVisible(ds23, ALICE);
      // this will be empty since now isVisible will not check the hierarchy privilege for the parent of the entity
      assertSingleVisibilityFailure(authEnforcementService, ds12, ALICE);
      assertSingleVisibilityFailure(authEnforcementService, ds22, ALICE);
      authEnforcementService.isVisible(ds11, BOB);
      authEnforcementService.isVisible(ds12, BOB);
      authEnforcementService.isVisible(ds22, BOB);
      assertSingleVisibilityFailure(authEnforcementService, ds21, BOB);
      assertSingleVisibilityFailure(authEnforcementService, ds23, BOB);
    }
  }

  @Test
  public void testIsVisible() throws IOException, AccessException {
    try (AccessControllerInstantiator accessControllerInstantiator =
           new AccessControllerInstantiator(CCONF, AUTH_CONTEXT_FACTORY)) {
      AccessController accessController = accessControllerInstantiator.get();
      NamespaceId ns1 = new NamespaceId("ns1");
      NamespaceId ns2 = new NamespaceId("ns2");
      DatasetId ds11 = ns1.dataset("ds11");
      DatasetId ds12 = ns1.dataset("ds12");
      DatasetId ds21 = ns2.dataset("ds21");
      DatasetId ds22 = ns2.dataset("ds22");
      DatasetId ds23 = ns2.dataset("ds33");
      Set<NamespaceId> namespaces = ImmutableSet.of(ns1, ns2);
      // Alice has access on ns1, ns2, ds11, ds21, ds23, Bob has access on ds11, ds12, ds22
      accessController.grant(Authorizable.fromEntityId(ns1), ALICE, Collections.singleton(StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ns2), ALICE, Collections.singleton(StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ds11), ALICE, Collections.singleton(StandardPermission.GET));
      accessController.grant(Authorizable.fromEntityId(ds11), BOB, Collections.singleton(StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ds21), ALICE, Collections.singleton(StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ds12), BOB, Collections.singleton(StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ds12), BOB, EnumSet.allOf(StandardPermission.class));
      accessController.grant(Authorizable.fromEntityId(ds21), ALICE, Collections.singleton(StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ds23), ALICE, Collections.singleton(StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ds22), BOB, Collections.singleton(StandardPermission.UPDATE));
      DefaultAccessEnforcer authEnforcementService =
        new DefaultAccessEnforcer(CCONF, accessControllerInstantiator);
      Assert.assertEquals(namespaces.size(), authEnforcementService.isVisible(namespaces, ALICE).size());
      // bob should also be able to list two namespaces since he has privileges on the dataset in both namespaces
      Assert.assertEquals(namespaces.size(), authEnforcementService.isVisible(namespaces, BOB).size());
      Set<DatasetId> expectedDatasetIds = ImmutableSet.of(ds11, ds21, ds23);
      Assert.assertEquals(expectedDatasetIds.size(), authEnforcementService.isVisible(expectedDatasetIds,
                                                                                      ALICE).size());
      expectedDatasetIds = ImmutableSet.of(ds12, ds22);
      // this will be empty since now isVisible will not check the hierarchy privilege for the parent of the entity
      Assert.assertEquals(Collections.EMPTY_SET, authEnforcementService.isVisible(expectedDatasetIds, ALICE));
      expectedDatasetIds = ImmutableSet.of(ds11, ds12, ds22);
      Assert.assertEquals(expectedDatasetIds.size(), authEnforcementService.isVisible(expectedDatasetIds, BOB).size());
      expectedDatasetIds = ImmutableSet.of(ds21, ds23);
      Assert.assertTrue(authEnforcementService.isVisible(expectedDatasetIds, BOB).isEmpty());
    }
  }

  @Test
  public void testSystemUser() throws IOException, AccessException {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    Principal systemUser =
      new Principal(UserGroupInformation.getCurrentUser().getShortUserName(), Principal.PrincipalType.USER);
    try (AccessControllerInstantiator accessControllerInstantiator =
           new AccessControllerInstantiator(cConfCopy, AUTH_CONTEXT_FACTORY)) {
      DefaultAccessEnforcer accessEnforcer =
        new DefaultAccessEnforcer(cConfCopy, accessControllerInstantiator);
      NamespaceId ns1 = new NamespaceId("ns1");
      accessEnforcer.enforce(NamespaceId.SYSTEM, systemUser, EnumSet.allOf(StandardPermission.class));
      accessEnforcer.isVisible(NamespaceId.SYSTEM, systemUser);
      Assert.assertEquals(ImmutableSet.of(NamespaceId.SYSTEM),
                          accessEnforcer.isVisible(ImmutableSet.of(ns1, NamespaceId.SYSTEM),
                                                          systemUser));
    }
  }

  private void verifyDisabled(CConfiguration cConf) throws IOException, AccessException {
    try (AccessControllerInstantiator accessControllerInstantiator =
           new AccessControllerInstantiator(cConf, AUTH_CONTEXT_FACTORY)) {
      DefaultAccessEnforcer authEnforcementService =
        new DefaultAccessEnforcer(cConf, accessControllerInstantiator);
      DatasetId ds = NS.dataset("ds");
      // All enforcement operations should succeed, since authorization is disabled
      accessControllerInstantiator.get().grant(Authorizable.fromEntityId(ds), BOB,
                                               ImmutableSet.of(StandardPermission.UPDATE));
      authEnforcementService.enforce(NS, ALICE, StandardPermission.UPDATE);
      authEnforcementService.enforce(ds, BOB, StandardPermission.UPDATE);
      authEnforcementService.isVisible(NS, BOB);
      authEnforcementService.isVisible(ds, BOB);
      Assert.assertEquals(2, authEnforcementService.isVisible(ImmutableSet.<EntityId>of(NS, ds), BOB).size());
    }
  }

  private void assertAuthorizationFailure(AccessEnforcer authEnforcementService,
                                          EntityId entityId, Principal principal,
                                          Permission permission) throws AccessException {
    try {
      authEnforcementService.enforce(entityId, principal, permission);
      Assert.fail(String.format("Expected %s to not have '%s' privilege on %s but it does.",
                                principal, permission, entityId));
    } catch (UnauthorizedException expected) {
      // expected
    }
  }

  private void assertAuthorizationFailure(AccessEnforcer authEnforcementService,
                                          EntityId entityId, Principal principal,
                                          Set<? extends Permission> permissions) throws AccessException {
    try {
      authEnforcementService.enforce(entityId, principal, permissions);
      Assert.fail(String.format("Expected %s to not have '%s' privileges on %s but it does.",
                                principal, permissions, entityId));
    } catch (UnauthorizedException expected) {
      // expected
    }
  }

  private void assertSingleVisibilityFailure(AccessEnforcer authEnforcementService,
                                          EntityId entityId, Principal principal) throws AccessException {
    try {
      authEnforcementService.isVisible(entityId, principal);
      Assert.fail(String.format("Expected %s to not have visibility privilege on %s but it does.",
                                principal, entityId));
    } catch (UnauthorizedException expected) {
      // expected
    }
  }
}
