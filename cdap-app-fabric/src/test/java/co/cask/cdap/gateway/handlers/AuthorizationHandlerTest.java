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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.client.AuthorizationClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.FeatureDisabledException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.proto.security.Role;
import co.cask.cdap.security.authorization.AuthorizationContextFactory;
import co.cask.cdap.security.authorization.AuthorizerInstantiatorService;
import co.cask.cdap.security.authorization.DefaultAuthorizationContext;
import co.cask.cdap.security.authorization.NoOpAdmin;
import co.cask.cdap.security.authorization.NoOpDatasetContext;
import co.cask.cdap.security.spi.authorization.AuthorizationContext;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.RoleAlreadyExistsException;
import co.cask.cdap.security.spi.authorization.RoleNotFoundException;
import co.cask.http.NettyHttpService;
import co.cask.tephra.TransactionFailureException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Tests for {@link AuthorizationHandler}.
 */
public class AuthorizationHandlerTest {

  private NettyHttpService service;
  private AuthorizationClient client;
  private static final AuthorizationContextFactory factory = new AuthorizationContextFactory() {
    @Override
    public AuthorizationContext create(Properties extensionProperties) {
      Transactional txnl = new Transactional() {
        @Override
        public void execute(TxRunnable runnable) throws TransactionFailureException {
          //no-op
        }
      };
      return new DefaultAuthorizationContext(extensionProperties, new NoOpDatasetContext(), new NoOpAdmin(), txnl);
    }
  };

  @Before
  public void setUp() throws UnknownHostException {
    CConfiguration conf = CConfiguration.create();
    conf.setBoolean(Constants.Security.Authorization.ENABLED, true);

    final InMemoryAuthorizer auth = new InMemoryAuthorizer();
    service = new CommonNettyHttpServiceBuilder(conf)
      .addHttpHandlers(ImmutableList.of(new AuthorizationHandler(
        new AuthorizerInstantiatorService(conf, factory) {
          @Override
          public Authorizer get() {
            return auth;
          }
        }, conf)))
      .build();
    service.startAndWait();

    client = new AuthorizationClient(
      ClientConfig.builder()
        .setConnectionConfig(
          ConnectionConfig.builder()
            .setHostname(service.getBindAddress().getHostName())
            .setPort(service.getBindAddress().getPort())
            .setSSLEnabled(false)
            .build())
        .build());
  }

  @After
  public void tearDown() {
    service.stopAndWait();
  }

  @Test
  public void testDisabled() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.setBoolean(Constants.Security.Authorization.ENABLED, false);

    NettyHttpService service = new CommonNettyHttpServiceBuilder(conf)
      .addHttpHandlers(ImmutableList.of(new AuthorizationHandler(
        new AuthorizerInstantiatorService(conf, factory) {
          @Override
          public Authorizer get() {
            return new InMemoryAuthorizer();
          }
        }, conf)))
      .build();
    service.startAndWait();

    final AuthorizationClient client = new AuthorizationClient(
      ClientConfig.builder()
        .setConnectionConfig(
          ConnectionConfig.builder()
            .setHostname(service.getBindAddress().getHostName())
            .setPort(service.getBindAddress().getPort())
            .setSSLEnabled(false)
            .build())
        .build());

    final NamespaceId ns1 = Ids.namespace("ns1");
    final Principal admin = new Principal("admin", Principal.PrincipalType.USER);
    final Role admins = new Role("admins");

    // Test that the right exception is thrown when any Authorization REST API is called when authorization is disabled
    verifyFeatureDisabled(new DisabledFeatureCaller() {
      @Override
      public void call() throws Exception {
        client.grant(ns1, admin, ImmutableSet.of(Action.READ));
      }
    });

    verifyFeatureDisabled(new DisabledFeatureCaller() {
      @Override
      public void call() throws Exception {
        client.revoke(ns1, admin, ImmutableSet.of(Action.READ));
      }
    });

    verifyFeatureDisabled(new DisabledFeatureCaller() {
      @Override
      public void call() throws Exception {
        client.revoke(ns1);
      }
    });

    verifyFeatureDisabled(new DisabledFeatureCaller() {
      @Override
      public void call() throws Exception {
        client.listPrivileges(admin);
      }
    });

    verifyFeatureDisabled(new DisabledFeatureCaller() {
      @Override
      public void call() throws Exception {
        client.addRoleToPrincipal(new Role("admins"), admin);
      }
    });

    verifyFeatureDisabled(new DisabledFeatureCaller() {
      @Override
      public void call() throws Exception {
        client.removeRoleFromPrincipal(admins, admin);
      }
    });

    verifyFeatureDisabled(new DisabledFeatureCaller() {
      @Override
      public void call() throws Exception {
        client.createRole(admins);
      }
    });

    verifyFeatureDisabled(new DisabledFeatureCaller() {
      @Override
      public void call() throws Exception {
        client.dropRole(admins);
      }
    });

    verifyFeatureDisabled(new DisabledFeatureCaller() {
      @Override
      public void call() throws Exception {
        client.listAllRoles();
      }
    });
  }

  @Test
  public void testRevokeEntityUserActions() throws Exception {
    NamespaceId ns1 = Ids.namespace("ns1");
    Principal admin = new Principal("admin", Principal.PrincipalType.ROLE);

    // grant() and revoke(EntityId, String, Set<Action>)
    verifyAuthFailure(ns1, admin, Action.READ);

    client.grant(ns1, admin, ImmutableSet.of(Action.READ));
    verifyAuthSuccess(ns1, admin, Action.READ);

    client.revoke(ns1, admin, ImmutableSet.of(Action.READ));
    verifyAuthFailure(ns1, admin, Action.READ);
  }

  @Test
  public void testRevokeEntityUser() throws Exception {
    NamespaceId ns1 = Ids.namespace("ns1");
    Principal admin = new Principal("admin", Principal.PrincipalType.GROUP);
    Principal bob = new Principal("bob", Principal.PrincipalType.USER);

    // grant() and revoke(EntityId, String)
    client.grant(ns1, admin, ImmutableSet.of(Action.READ));
    client.grant(ns1, bob, ImmutableSet.of(Action.READ));
    verifyAuthSuccess(ns1, admin, Action.READ);
    verifyAuthSuccess(ns1, bob, Action.READ);

    client.revoke(ns1, admin, EnumSet.allOf(Action.class));
    verifyAuthFailure(ns1, admin, Action.READ);
    verifyAuthSuccess(ns1, bob, Action.READ);
  }

  @Test
  public void testRevokeEntity() throws Exception {
    NamespaceId ns1 = Ids.namespace("ns1");
    NamespaceId ns2 = Ids.namespace("ns2");
    Principal admin = new Principal("admin", Principal.PrincipalType.GROUP);
    Principal bob = new Principal("bob", Principal.PrincipalType.USER);

    // grant() and revoke(EntityId)
    client.grant(ns1, admin, ImmutableSet.of(Action.READ));
    client.grant(ns1, bob, ImmutableSet.of(Action.READ));
    client.grant(ns2, admin, ImmutableSet.of(Action.READ));
    verifyAuthSuccess(ns1, admin, Action.READ);
    verifyAuthSuccess(ns1, bob, Action.READ);
    verifyAuthSuccess(ns2, admin, Action.READ);

    client.revoke(ns1);
    verifyAuthFailure(ns1, admin, Action.READ);
    verifyAuthFailure(ns1, bob, Action.READ);
    verifyAuthSuccess(ns2, admin, Action.READ);
  }

  @Test
  public void testRBAC() throws Exception {
    Role admins = new Role("admins");
    Role engineers = new Role("engineers");
    // create a role
    client.createRole(admins);
    // add another role
    client.createRole(engineers);

    // listing role should show the added role
    Set<Role> roles = client.listAllRoles();
    Assert.assertEquals(Sets.newHashSet(admins, engineers), roles);

    // creating a role which already exists should throw an exception
    try {
      client.createRole(admins);
      Assert.fail(String.format("Created a role %s which already exists. Should have failed.", admins.getName()));
    } catch (RoleAlreadyExistsException expected) {
      // expected
    }

    // drop an existing role
    client.dropRole(admins);

    // the list should not have the dropped role
    roles = client.listAllRoles();
    Assert.assertEquals(Sets.newHashSet(engineers), roles);

    // dropping a non-existing role should throw exception
    try {
      client.dropRole(admins);
      Assert.fail(String.format("Dropped a role %s which does not exists. Should have failed.", admins.getName()));
    } catch (RoleNotFoundException expected) {
      // expected
    }

    // add an user to an existing role
    Principal spiderman = new Principal("spiderman", Principal.PrincipalType.USER);
    client.addRoleToPrincipal(engineers, spiderman);

    // add an user to an non-existing role should throw an exception
    try {
      client.addRoleToPrincipal(admins, spiderman);
      Assert.fail(String.format("Added role %s to principal %s. Should have failed.", admins, spiderman));
    } catch (RoleNotFoundException expected) {
      // expected
    }

    // check listing roles for spiderman have engineers role
    Assert.assertEquals(Sets.newHashSet(engineers), client.listRoles(spiderman));

    // authorization checks with roles
    NamespaceId ns1 = Ids.namespace("ns1");

    // check that spiderman who has engineers roles cannot read from ns1
    verifyAuthFailure(ns1, spiderman, Action.READ);

    // give a permission to engineers role
    client.grant(ns1, engineers, ImmutableSet.of(Action.READ));

    // check that a spiderman who has engineers role has access
    verifyAuthSuccess(ns1, spiderman, Action.READ);

    // list privileges for spiderman should have read action on ns1
    Assert.assertEquals(Sets.newHashSet(new Privilege(ns1, Action.READ)), client.listPrivileges(spiderman));

    // revoke action from the role
    client.revoke(ns1, engineers, ImmutableSet.of(Action.READ));

    // now the privileges for spiderman should be empty
    Assert.assertEquals(new HashSet<>(), client.listPrivileges(spiderman));

    // check that the user of this role is not authorized to do the revoked operation
    verifyAuthFailure(ns1, spiderman, Action.READ);

    // remove an user from a existing role
    client.removeRoleFromPrincipal(engineers, spiderman);

    // check listing roles for spiderman should be empty
    Assert.assertEquals(new HashSet<>(), client.listRoles(spiderman));

    // remove an user from a non-existing role should throw exception
    try {
      client.removeRoleFromPrincipal(admins, spiderman);
      Assert.fail(String.format("Removed non-existing role %s from principal %s. Should have failed.", admins,
                                spiderman));
    } catch (RoleNotFoundException expected) {
      // expected
    }
  }

  /**
   * Interface to centralize testing the exception thrown when Authorization is disabled.
   */
  private interface DisabledFeatureCaller {
    void call() throws Exception;
  }

  /**
   * Calls a {@link DisabledFeatureCaller} and verifies that the right exception was thrown.
   *
   * @param caller the {@link DisabledFeatureCaller} that wraps the operation to test
   */
  private void verifyFeatureDisabled(DisabledFeatureCaller caller) throws Exception {
    try {
      caller.call();
    } catch (FeatureDisabledException expected) {
      Assert.assertEquals("Authorization", expected.getFeature());
      Assert.assertEquals("cdap-site.xml", expected.getConfigFile());
      Assert.assertEquals(Constants.Security.Authorization.ENABLED, expected.getEnableConfigKey());
      Assert.assertEquals("true", expected.getEnableConfigValue());
    }
  }

  private void verifyAuthSuccess(EntityId entity, Principal principal, Action action) throws Exception {
    Set<Privilege> privileges = client.listPrivileges(principal);
    Privilege privilegeToCheck = new Privilege(entity, action);
    Assert.assertTrue(
      String.format(
        "Expected principal %s to have the privilege %s, but found that it did not.", principal, privilegeToCheck
      ),
      privileges.contains(privilegeToCheck)
    );
  }

  private void verifyAuthFailure(EntityId entity, Principal principal, Action action) throws Exception {
    Set<Privilege> privileges = client.listPrivileges(principal);
    Privilege privilegeToCheck = new Privilege(entity, action);
    Assert.assertFalse(
      String.format(
        "Expected principal %s to not have the privilege %s, but found that it did.", principal, privilegeToCheck
      ),
      privileges.contains(privilegeToCheck)
    );
  }
}
