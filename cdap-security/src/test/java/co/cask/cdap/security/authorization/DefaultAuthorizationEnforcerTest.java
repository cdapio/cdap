/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Authorizable;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.collect.ImmutableSet;
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
 * Tests for {@link DefaultAuthorizationEnforcer}.
 */
public class DefaultAuthorizationEnforcerTest extends AuthorizationTestBase {

  private static final Principal ALICE = new Principal("alice", Principal.PrincipalType.USER);
  private static final Principal BOB = new Principal("bob", Principal.PrincipalType.USER);
  private static final NamespaceId NS = new NamespaceId("ns");
  private static final ApplicationId APP = NS.app("app");

  @BeforeClass
  public static void setupClass() throws IOException {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, InMemoryAuthorizer.class.getName());
    Location externalAuthJar = AppJarHelper.createDeploymentJar(locationFactory, InMemoryAuthorizer.class, manifest);
    CCONF.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, externalAuthJar.toString());
  }

  @Test
  public void testAuthenticationDisabled() throws Exception {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    cConfCopy.setBoolean(Constants.Security.ENABLED, false);
    verifyDisabled(cConfCopy);
  }

  @Test
  public void testAuthorizationDisabled() throws Exception {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    cConfCopy.setBoolean(Constants.Security.Authorization.ENABLED, false);
    verifyDisabled(cConfCopy);
  }

  @Test
  public void testPropagationDisabled() throws Exception {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    try (AuthorizerInstantiator authorizerInstantiator = new AuthorizerInstantiator(cConfCopy,
                                                                                    AUTH_CONTEXT_FACTORY)) {
      DefaultAuthorizationEnforcer authorizationEnforcer =
        new DefaultAuthorizationEnforcer(cConfCopy, authorizerInstantiator);
      authorizerInstantiator.get().grant(Authorizable.fromEntityId(NS), ALICE, ImmutableSet.of(Action.ADMIN));
      authorizationEnforcer.enforce(NS, ALICE, Action.ADMIN);
      try {
        authorizationEnforcer.enforce(APP, ALICE, Action.ADMIN);
        Assert.fail("Alice should not have ADMIN privilege on the APP.");
      } catch (UnauthorizedException ignored) {
        // expected
      }
    }
  }

  @Test
  public void testAuthEnforce() throws Exception {
    try (AuthorizerInstantiator authorizerInstantiator = new AuthorizerInstantiator(CCONF, AUTH_CONTEXT_FACTORY)) {
      Authorizer authorizer = authorizerInstantiator.get();
      DefaultAuthorizationEnforcer authEnforcementService =
        new DefaultAuthorizationEnforcer(CCONF, authorizerInstantiator);
      // update privileges for alice. Currently alice has not been granted any privileges.
      assertAuthorizationFailure(authEnforcementService, NS, ALICE, Action.ADMIN);

      // grant some test privileges
      DatasetId ds = NS.dataset("ds");
      authorizer.grant(Authorizable.fromEntityId(NS), ALICE, ImmutableSet.of(Action.READ, Action.WRITE));
      authorizer.grant(Authorizable.fromEntityId(ds), BOB, ImmutableSet.of(Action.ADMIN));

      // auth enforcement for alice should succeed on ns for actions read and write
      authEnforcementService.enforce(NS, ALICE, ImmutableSet.of(Action.READ, Action.WRITE));
      assertAuthorizationFailure(authEnforcementService, NS, ALICE, EnumSet.allOf(Action.class));
      // alice do not have READ or WRITE on the dataset, so authorization should fail
      assertAuthorizationFailure(authEnforcementService, ds, ALICE, Action.READ);
      assertAuthorizationFailure(authEnforcementService, ds, ALICE, Action.WRITE);

      // Alice doesn't have Admin right on NS, hence should fail.
      assertAuthorizationFailure(authEnforcementService, NS, ALICE, Action.ADMIN);
      // bob enforcement should succeed since we grant him admin privilege
      authEnforcementService.enforce(ds, BOB, Action.ADMIN);
      // revoke all of alice's privileges
      authorizer.revoke(Authorizable.fromEntityId(NS), ALICE, ImmutableSet.of(Action.READ));
      try {
        authEnforcementService.enforce(NS, ALICE, Action.READ);
        Assert.fail(String.format("Expected %s to not have '%s' privilege on %s but it does.",
                                  ALICE, Action.READ, NS));
      } catch (UnauthorizedException ignored) {
        // expected
      }
      authorizer.revoke(Authorizable.fromEntityId(NS));

      assertAuthorizationFailure(authEnforcementService, NS, ALICE, Action.READ);
      assertAuthorizationFailure(authEnforcementService, NS, ALICE, Action.WRITE);
      authEnforcementService.enforce(ds, BOB, Action.ADMIN);
    }
  }

  @Test
  public void testIsVisible() throws Exception {
    try (AuthorizerInstantiator authorizerInstantiator = new AuthorizerInstantiator(CCONF, AUTH_CONTEXT_FACTORY)) {
      Authorizer authorizer = authorizerInstantiator.get();
      NamespaceId ns1 = new NamespaceId("ns1");
      NamespaceId ns2 = new NamespaceId("ns2");
      DatasetId ds11 = ns1.dataset("ds11");
      DatasetId ds12 = ns1.dataset("ds12");
      DatasetId ds21 = ns2.dataset("ds21");
      DatasetId ds22 = ns2.dataset("ds22");
      DatasetId ds23 = ns2.dataset("ds33");
      Set<NamespaceId> namespaces = ImmutableSet.of(ns1, ns2);
      // Alice has access on ns1, ns2, ds11, ds21, ds23, Bob has access on ds11, ds12, ds22
      authorizer.grant(Authorizable.fromEntityId(ns1), ALICE, Collections.singleton(Action.WRITE));
      authorizer.grant(Authorizable.fromEntityId(ns2), ALICE, Collections.singleton(Action.ADMIN));
      authorizer.grant(Authorizable.fromEntityId(ds11), ALICE, Collections.singleton(Action.READ));
      authorizer.grant(Authorizable.fromEntityId(ds11), BOB, Collections.singleton(Action.ADMIN));
      authorizer.grant(Authorizable.fromEntityId(ds21), ALICE, Collections.singleton(Action.WRITE));
      authorizer.grant(Authorizable.fromEntityId(ds12), BOB, Collections.singleton(Action.WRITE));
      authorizer.grant(Authorizable.fromEntityId(ds12), BOB, EnumSet.allOf(Action.class));
      authorizer.grant(Authorizable.fromEntityId(ds21), ALICE, Collections.singleton(Action.WRITE));
      authorizer.grant(Authorizable.fromEntityId(ds23), ALICE, Collections.singleton(Action.ADMIN));
      authorizer.grant(Authorizable.fromEntityId(ds22), BOB, Collections.singleton(Action.ADMIN));
      DefaultAuthorizationEnforcer authEnforcementService =
        new DefaultAuthorizationEnforcer(CCONF, authorizerInstantiator);
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
  public void testSystemUser() throws Exception {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    Principal systemUser =
      new Principal(UserGroupInformation.getCurrentUser().getShortUserName(), Principal.PrincipalType.USER);
    try (AuthorizerInstantiator authorizerInstantiator = new AuthorizerInstantiator(cConfCopy, AUTH_CONTEXT_FACTORY)) {
      Authorizer authorizer = authorizerInstantiator.get();
      DefaultAuthorizationEnforcer authorizationEnforcer =
        new DefaultAuthorizationEnforcer(cConfCopy, authorizerInstantiator);
      NamespaceId ns1 = new NamespaceId("ns1");
      authorizationEnforcer.enforce(NamespaceId.SYSTEM, systemUser, EnumSet.allOf(Action.class));
      Assert.assertEquals(ImmutableSet.of(NamespaceId.SYSTEM),
                          authorizationEnforcer.isVisible(ImmutableSet.of(ns1, NamespaceId.SYSTEM),
                                                          systemUser));
    }
  }

  private void verifyDisabled(CConfiguration cConf) throws Exception {
    try (AuthorizerInstantiator authorizerInstantiator = new AuthorizerInstantiator(cConf, AUTH_CONTEXT_FACTORY)) {
      DefaultAuthorizationEnforcer authEnforcementService =
        new DefaultAuthorizationEnforcer(cConf, authorizerInstantiator);
      DatasetId ds = NS.dataset("ds");
      // All enforcement operations should succeed, since authorization is disabled
      authorizerInstantiator.get().grant(Authorizable.fromEntityId(ds), BOB, ImmutableSet.of(Action.ADMIN));
      authEnforcementService.enforce(NS, ALICE, Action.ADMIN);
      authEnforcementService.enforce(ds, BOB, Action.ADMIN);
      Assert.assertEquals(2, authEnforcementService.isVisible(ImmutableSet.<EntityId>of(NS, ds), BOB).size());
    }
  }

  private void assertAuthorizationFailure(AuthorizationEnforcer authEnforcementService,
                                          EntityId entityId, Principal principal, Action action) throws Exception {
    try {
      authEnforcementService.enforce(entityId, principal, action);
      Assert.fail(String.format("Expected %s to not have '%s' privilege on %s but it does.",
                                principal, action, entityId));
    } catch (UnauthorizedException expected) {
      // expected
    }
  }

  private void assertAuthorizationFailure(AuthorizationEnforcer authEnforcementService,
                                          EntityId entityId, Principal principal,
                                          Set<Action> actions) throws Exception {
    try {
      authEnforcementService.enforce(entityId, principal, actions);
      Assert.fail(String.format("Expected %s to not have '%s' privileges on %s but it does.",
                                principal, actions, entityId));
    } catch (UnauthorizedException expected) {
      // expected
    }
  }
}
