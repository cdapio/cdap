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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Role;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Tests invalidation of caches in {@link DefaultAuthorizationEnforcementService} and
 * {@link PrivilegesFetcherProxyService} when privileges are updated.
 */
public class AuthorizationCacheInvalidationTest extends AuthorizationTestBase {
  @BeforeClass
  public static void setupClass() throws IOException {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, InMemoryAuthorizer.class.getName());
    Location externalAuthJar = AppJarHelper.createDeploymentJar(locationFactory, InMemoryAuthorizer.class, manifest);
    CCONF.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, externalAuthJar.toString());
  }

  @Test
  public void testUserPrivileges() throws Exception {
    try (AuthorizerInstantiator authorizerInstantiator = new AuthorizerInstantiator(CCONF, AUTH_CONTEXT_FACTORY)) {
      Authorizer authorizer = authorizerInstantiator.get();
      DefaultPrivilegesFetcherProxyService privilegesFetcherProxyService =
        new DefaultPrivilegesFetcherProxyService(authorizer, CCONF, AUTH_CONTEXT);
      privilegesFetcherProxyService.startAndWait();
      try {
        DefaultAuthorizationEnforcementService authorizationEnforcementService =
          new DefaultAuthorizationEnforcementService(authorizer, CCONF, AUTH_CONTEXT);
        authorizationEnforcementService.startAndWait();
        int initialCacheSize = privilegesFetcherProxyService.getCache().size();
        Assert.assertEquals(initialCacheSize, authorizationEnforcementService.getCache().size());
        try {
          Principal alice = new Principal("alice", Principal.PrincipalType.USER);
          NamespaceId ns = new NamespaceId("ns");
          authorizer.grant(ns, alice, Collections.singleton(Action.ADMIN));
          // update cache
          privilegesFetcherProxyService.listPrivileges(alice);
          authorizationEnforcementService.createFilter(alice);

          // alice's privileges would have been cached
          Assert.assertEquals(initialCacheSize + 1, privilegesFetcherProxyService.getCache().size());
          Assert.assertEquals(initialCacheSize + 1, authorizationEnforcementService.getCache().size());

          PrivilegesManager privilegesManager = new DefaultPrivilegesManager(
            authorizerInstantiator, authorizationEnforcementService, privilegesFetcherProxyService);
          privilegesManager.grant(ns, alice, Collections.singleton(Action.ADMIN));

          // alice's privileges would have been invalidated
          Assert.assertEquals(initialCacheSize, privilegesFetcherProxyService.getCache().size());
          Assert.assertEquals(initialCacheSize, authorizationEnforcementService.getCache().size());
        } finally {
          authorizationEnforcementService.stopAndWait();
        }

      } finally {
        privilegesFetcherProxyService.stopAndWait();
      }
    }
  }

  @Test
  public void testRoleBasedPrivileges() throws Exception {
    Principal alice = new Principal("alice", Principal.PrincipalType.USER);
    Principal bob = new Principal("bob", Principal.PrincipalType.USER);
    Role admins = new Role("admins");
    NamespaceId ns = new NamespaceId("ns");
    try (AuthorizerInstantiator authorizerInstantiator = new AuthorizerInstantiator(CCONF, AUTH_CONTEXT_FACTORY)) {
      Authorizer authorizer = authorizerInstantiator.get();
      DefaultPrivilegesFetcherProxyService privilegesFetcherProxyService =
        new DefaultPrivilegesFetcherProxyService(authorizer, CCONF, AUTH_CONTEXT);
      privilegesFetcherProxyService.startAndWait();
      try {
        DefaultAuthorizationEnforcementService authorizationEnforcementService =
          new DefaultAuthorizationEnforcementService(authorizer, CCONF, AUTH_CONTEXT);
        authorizationEnforcementService.startAndWait();
        int initialCacheSize = privilegesFetcherProxyService.getCache().size();
        Assert.assertEquals(initialCacheSize, authorizationEnforcementService.getCache().size());
        try {
          authorizer.grant(ns, alice, Collections.singleton(Action.ADMIN));
          authorizer.grant(ns, bob, Collections.singleton(Action.WRITE));
          // update cache
          privilegesFetcherProxyService.listPrivileges(alice);
          authorizationEnforcementService.createFilter(alice);
          privilegesFetcherProxyService.listPrivileges(bob);
          authorizationEnforcementService.createFilter(bob);

          // alice and bob's privileges would have been cached
          Assert.assertEquals(initialCacheSize + 2, privilegesFetcherProxyService.getCache().size());
          Assert.assertEquals(initialCacheSize + 2, authorizationEnforcementService.getCache().size());

          // create role and add it to alice and bob
          authorizer.createRole(admins);
          authorizer.addRoleToPrincipal(admins, alice);
          authorizer.addRoleToPrincipal(admins, bob);
          authorizer.grant(ns, admins, Collections.singleton(Action.READ));
          PrivilegesManager privilegesManager = new DefaultPrivilegesManager(
            authorizerInstantiator, authorizationEnforcementService, privilegesFetcherProxyService);
          // updating privileges of the role should invalidate cache entries of both alice and bob
          privilegesManager.grant(ns, admins, Collections.singleton(Action.ADMIN));

          // alice's privileges would have been invalidated
          Assert.assertEquals(initialCacheSize, privilegesFetcherProxyService.getCache().size());
          Assert.assertEquals(initialCacheSize, authorizationEnforcementService.getCache().size());
        } finally {
          authorizationEnforcementService.stopAndWait();
        }

      } finally {
        privilegesFetcherProxyService.stopAndWait();
      }
    }
  }
}
