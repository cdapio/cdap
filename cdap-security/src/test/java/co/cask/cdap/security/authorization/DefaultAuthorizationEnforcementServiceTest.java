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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.PrivilegesFetcher;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Service;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Tests for {@link DefaultAuthorizationEnforcementService}.
 */
public class DefaultAuthorizationEnforcementServiceTest extends AuthorizationTestBase {

  private static final Principal ALICE = new Principal("alice", Principal.PrincipalType.USER);
  private static final Principal BOB = new Principal("bob", Principal.PrincipalType.USER);
  private static final NamespaceId NS = new NamespaceId("ns");

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
  public void testCachingDisabled() throws Exception {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    cConfCopy.setBoolean(Constants.Security.Authorization.CACHE_ENABLED, false);
    try (AuthorizerInstantiator authorizerInstantiator = new AuthorizerInstantiator(cConfCopy, AUTH_CONTEXT_FACTORY)) {
      DefaultAuthorizationEnforcementService authEnforcementService =
        new DefaultAuthorizationEnforcementService(authorizerInstantiator.get(), cConfCopy);
      authEnforcementService.startAndWait();
      try {
        authorizerInstantiator.get().grant(NS, ALICE, ImmutableSet.of(Action.ADMIN));
        authEnforcementService.enforce(NS, ALICE, Action.ADMIN);
        Assert.assertTrue(authEnforcementService.getCache().isEmpty());
      } finally {
        authEnforcementService.stopAndWait();
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidCacheTTLConfig() throws Exception {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    cConfCopy.setInt(Constants.Security.Authorization.CACHE_TTL_SECS, -1);
    try (AuthorizerInstantiator authorizerInstantiator = new AuthorizerInstantiator(cConfCopy, AUTH_CONTEXT_FACTORY)) {
      new DefaultAuthorizationEnforcementService(authorizerInstantiator.get(), cConfCopy);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidCacheRefreshConfig() throws Exception {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    cConfCopy.setInt(Constants.Security.Authorization.CACHE_REFRESH_INTERVAL_SECS, -1);
    try (AuthorizerInstantiator authorizerInstantiator = new AuthorizerInstantiator(cConfCopy, AUTH_CONTEXT_FACTORY)) {
      new DefaultAuthorizationEnforcementService(authorizerInstantiator.get(), cConfCopy);
    }
  }

  @Test
  public void testAuthCacheEnforce() throws Exception {
    try (AuthorizerInstantiator authorizerInstantiator = new AuthorizerInstantiator(CCONF, AUTH_CONTEXT_FACTORY)) {
      Authorizer authorizer = authorizerInstantiator.get();
      DefaultAuthorizationEnforcementService authEnforcementService =
        new DefaultAuthorizationEnforcementService(authorizer, CCONF);
      authEnforcementService.startAndWait();
      try {
        // update privileges for alice. Currently alice has not been granted any privileges.
        assertAuthorizationFailure(authEnforcementService, NS, ALICE, Action.ADMIN);
        Assert.assertTrue(authEnforcementService.getCache().get(ALICE).isEmpty());

        // grant some test privileges
        DatasetId ds = NS.dataset("ds");
        authorizer.grant(NS, ALICE, ImmutableSet.of(Action.READ, Action.WRITE));
        authorizer.grant(ds, BOB, ImmutableSet.of(Action.ADMIN));
        // Running an iteration should update alice's privileges
        authEnforcementService.runOneIteration();

        Assert.assertEquals(EnumSet.of(Action.READ, Action.WRITE),
                            authEnforcementService.getCache().get(ALICE).get(NS));

        // auth enforcement for alice should succeed on ns for actions read and write
        authEnforcementService.enforce(NS, ALICE, ImmutableSet.of(Action.READ, Action.WRITE));
        assertAuthorizationFailure(authEnforcementService, NS, ALICE, EnumSet.allOf(Action.class));
        // since Alice has READ/WRITE on the NS, everything under that should have READ/WRITE as well.
        authEnforcementService.enforce(ds, ALICE, Action.READ);
        authEnforcementService.enforce(ds, ALICE, Action.WRITE);

        // Alice don't have Admin right on NS, hence should fail.
        assertAuthorizationFailure(authEnforcementService, NS, ALICE, Action.ADMIN);
        // also, even though bob's privileges were never updated, auth enforcement for bob should not fail, because the
        // LoadingCache should make a blocking call to retrieve his privileges
        authEnforcementService.enforce(ds, BOB, Action.ADMIN);
        // revoke all of alice's privileges
        authorizer.revoke(NS);
        // run another iteration. Both alice and bob's privileges should have been updated in the cache now
        authEnforcementService.runOneIteration();

        Assert.assertTrue(authEnforcementService.getCache().get(ALICE).isEmpty());
        Assert.assertEquals(EnumSet.of(Action.ADMIN), authEnforcementService.getCache().get(BOB).get(ds));
        assertAuthorizationFailure(authEnforcementService, NS, ALICE, Action.READ);
        assertAuthorizationFailure(authEnforcementService, NS, ALICE, Action.WRITE);
        authEnforcementService.enforce(ds, BOB, Action.ADMIN);
      } finally {
        authEnforcementService.stopAndWait();
      }
    }
  }

  @Test
  public void testAuthCacheFilter() throws Exception {
    try (AuthorizerInstantiator authorizerInstantiator = new AuthorizerInstantiator(CCONF, AUTH_CONTEXT_FACTORY)) {
      Authorizer authorizer = authorizerInstantiator.get();
      NamespaceId ns1 = new NamespaceId("ns1");
      NamespaceId ns2 = new NamespaceId("ns2");
      DatasetId ds11 = ns1.dataset("ds1");
      DatasetId ds12 = ns1.dataset("ds2");
      DatasetId ds21 = ns2.dataset("ds1");
      DatasetId ds22 = ns2.dataset("ds2");
      DatasetId ds23 = ns2.dataset("ds3");
      Set<NamespaceId> namespaces = ImmutableSet.of(ns1, ns2);
      authorizer.grant(ns1, ALICE, Collections.singleton(Action.WRITE));
      authorizer.grant(ns2, ALICE, Collections.singleton(Action.ADMIN));
      authorizer.grant(ds11, ALICE, Collections.singleton(Action.READ));
      authorizer.grant(ds11, BOB, Collections.singleton(Action.ADMIN));
      authorizer.grant(ds21, ALICE, Collections.singleton(Action.WRITE));
      authorizer.grant(ds12, BOB, Collections.singleton(Action.WRITE));
      authorizer.grant(ds12, BOB, Collections.singleton(Action.ALL));
      authorizer.grant(ds21, ALICE, Collections.singleton(Action.WRITE));
      authorizer.grant(ds23, ALICE, Collections.singleton(Action.ADMIN));
      authorizer.grant(ds22, BOB, Collections.singleton(Action.ADMIN));
      DefaultAuthorizationEnforcementService authEnforcementService =
        new DefaultAuthorizationEnforcementService(authorizer, CCONF);
      authEnforcementService.startAndWait();
      try {
        Predicate<EntityId> aliceFilter = authEnforcementService.createFilter(ALICE);
        for (NamespaceId namespace : namespaces) {
          Assert.assertTrue(aliceFilter.apply(namespace));
        }
        Predicate<EntityId> bobFilter = authEnforcementService.createFilter(BOB);
        for (NamespaceId namespace : namespaces) {
          Assert.assertFalse(bobFilter.apply(namespace));
        }
        for (DatasetId datasetId : ImmutableSet.of(ds11, ds21, ds23)) {
          Assert.assertTrue(aliceFilter.apply(datasetId));
        }
        for (DatasetId datasetId : ImmutableSet.of(ds12, ds22)) {
          Assert.assertTrue(aliceFilter.apply(datasetId));
        }
        for (DatasetId datasetId : ImmutableSet.of(ds11, ds12, ds22)) {
          Assert.assertTrue(bobFilter.apply(datasetId));
        }
        for (DatasetId datasetId : ImmutableSet.of(ds21, ds23)) {
          Assert.assertFalse(bobFilter.apply(datasetId));
        }
      } finally {
        authEnforcementService.stopAndWait();
      }
    }
  }

  @Test
  public void testResiliency() throws Exception {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    cConfCopy.setInt(Constants.Security.Authorization.CACHE_REFRESH_INTERVAL_SECS, 1);
    CountDownLatch countDownLatch = new CountDownLatch(10);
    DefaultAuthorizationEnforcementService authorizationEnforcementService =
      new DefaultAuthorizationEnforcementService(new FailingPrivilegesFetcher(countDownLatch), cConfCopy);
    Map<Principal, Map<EntityId, Set<Action>>> cache = authorizationEnforcementService.getCache();
    cache.put(new Principal("bob", Principal.PrincipalType.USER), Collections.<EntityId, Set<Action>>emptyMap());
    cache.put(new Principal("tom", Principal.PrincipalType.USER), Collections.<EntityId, Set<Action>>emptyMap());
    authorizationEnforcementService.startAndWait();
    try {
      // CountDownLatch is initialized to 10 and we have 2 users in the cache, so it should countdown twice in every
      // iteration. That way, the service runs for 5 iterations, throwing exceptions in every iteration.
      countDownLatch.await();
      Assert.assertEquals(
        String.format("Expected authorization enforcement service to be %s, but it is %s",
                      Service.State.RUNNING, authorizationEnforcementService.state()),
        Service.State.RUNNING,
        authorizationEnforcementService.state()
      );
    } finally {
      authorizationEnforcementService.stopAndWait();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoSuperUsers() throws Exception {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    cConfCopy.unset(Constants.Security.Authorization.SUPERUSERS);
    try (AuthorizerInstantiator authorizerInstantiator = new AuthorizerInstantiator(cConfCopy, AUTH_CONTEXT_FACTORY)) {
      Authorizer authorizer = authorizerInstantiator.get();
      new DefaultAuthorizationEnforcementService(authorizer, cConfCopy);
    }
  }

  @Test
  public void testSuperUsers() throws Exception {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    Principal superUser = new Principal("tom", Principal.PrincipalType.USER);
    cConfCopy.set(Constants.Security.Authorization.SUPERUSERS, superUser.getName());
    try (AuthorizerInstantiator authorizerInstantiator = new AuthorizerInstantiator(cConfCopy, AUTH_CONTEXT_FACTORY)) {
      Authorizer authorizer = authorizerInstantiator.get();
      DefaultAuthorizationEnforcementService authorizationEnforcementService =
        new DefaultAuthorizationEnforcementService(authorizer, cConfCopy);
      NamespaceId ns1 = new NamespaceId("ns1");
      DatasetId ds1 = ns1.dataset("ds1");
      StreamId s1 = ns1.stream("s1");
      authorizationEnforcementService.startAndWait();
      try {
        authorizationEnforcementService.enforce(ns1, superUser, Action.ALL);
        authorizationEnforcementService.enforce(ds1, superUser, ImmutableSet.of(Action.WRITE, Action.READ));
        Predicate<EntityId> filter = authorizationEnforcementService.createFilter(superUser);
        Assert.assertTrue(filter.apply(ns1));
        Assert.assertTrue(filter.apply(ds1));
        Assert.assertTrue(filter.apply(s1));
      } finally {
        authorizationEnforcementService.stopAndWait();
      }
    }
  }

  private void verifyDisabled(CConfiguration cConf) throws Exception {
    try (AuthorizerInstantiator authorizerInstantiator = new AuthorizerInstantiator(cConf, AUTH_CONTEXT_FACTORY)) {
      DefaultAuthorizationEnforcementService authEnforcementService =
        new DefaultAuthorizationEnforcementService(authorizerInstantiator.get(), cConf);
      authEnforcementService.startAndWait();
      try {
        DatasetId ds = NS.dataset("ds");
        Assert.assertTrue(authEnforcementService.getCache().isEmpty());
        // despite the cache being empty, any enforcement operations should succeed, since authorization is disabled
        authEnforcementService.enforce(NS, ALICE, Action.ADMIN);
        authEnforcementService.enforce(ds, BOB, Action.ADMIN);
        Predicate<EntityId> filter = authEnforcementService.createFilter(BOB);
        Assert.assertTrue(filter.apply(NS));
        Assert.assertTrue(filter.apply(ds));
      } finally {
        authEnforcementService.stopAndWait();
      }
    }
  }

  private void assertAuthorizationFailure(DefaultAuthorizationEnforcementService authEnforcementService,
                                          EntityId entityId, Principal principal, Action action) throws Exception {
    try {
      authEnforcementService.enforce(entityId, principal, action);
      Assert.fail(String.format("Expected %s to not have '%s' privilege on %s but it does.",
                                principal, action, entityId));
    } catch (UnauthorizedException expected) {
      // expected
    }
  }

  private void assertAuthorizationFailure(DefaultAuthorizationEnforcementService authEnforcementService,
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

  private static final class FailingPrivilegesFetcher implements PrivilegesFetcher {

    private final CountDownLatch countDownLatch;

    private FailingPrivilegesFetcher(CountDownLatch countDownLatch) {
      this.countDownLatch = countDownLatch;
    }

    @Override
    public Set<Privilege> listPrivileges(Principal principal) throws Exception {
      countDownLatch.countDown();
      throw new UnsupportedOperationException(
        String.format("Deliberately failing list privileges for %s to test resiliency.", principal));
    }
  }
}
