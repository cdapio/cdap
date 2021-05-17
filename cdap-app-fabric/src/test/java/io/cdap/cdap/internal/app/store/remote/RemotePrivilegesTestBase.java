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

package io.cdap.cdap.internal.app.store.remote;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.services.AppFabricServer;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.security.Action;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Privilege;
import io.cdap.cdap.security.authorization.AccessControllerWrapper;
import io.cdap.cdap.security.authorization.AccessEnforcerWrapper;
import io.cdap.cdap.security.authorization.InMemoryAuthorizer;
import io.cdap.cdap.security.authorization.RemoteAccessEnforcer;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import io.cdap.cdap.security.spi.authorization.PrivilegesManager;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Tests for remote implementations of {@link AuthorizationEnforcer} and {@link PrivilegesManager}.
 * These are in app-fabric, because we need to start app-fabric in these tests.
 */
@SuppressWarnings("WeakerAccess")
public abstract class RemotePrivilegesTestBase {
  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  protected static final Principal ALICE = new Principal("alice", Principal.PrincipalType.USER);
  protected static final Principal BOB = new Principal("bob", Principal.PrincipalType.USER);
  protected static final Principal CAROL = new Principal("carol", Principal.PrincipalType.USER);
  protected static final NamespaceId NS = new NamespaceId("ns");
  protected static final ApplicationId APP = NS.app("app");
  protected static final ProgramId PROGRAM = APP.program(ProgramType.SERVICE, "ser");
  private static final int CACHE_TIMEOUT = 3;

  protected static AccessEnforcer accessEnforcer;
  protected static AuthorizationEnforcer authorizationEnforcer;
  protected static PrivilegesManager privilegesManager;
  protected static CConfiguration cConf = CConfiguration.create();

  private static DiscoveryServiceClient discoveryService;
  private static AppFabricServer appFabricServer;

  protected static void setup() throws IOException, InterruptedException {
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMPORARY_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.KERBEROS_ENABLED, false);
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
    cConf.setInt(Constants.Security.Authorization.CACHE_TTL_SECS, CACHE_TIMEOUT);
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, InMemoryAuthorizer.class.getName());
    LocationFactory locationFactory = new LocalLocationFactory(TEMPORARY_FOLDER.newFolder());
    Location externalAuthJar = AppJarHelper.createDeploymentJar(locationFactory, InMemoryAuthorizer.class, manifest);
    cConf.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, externalAuthJar.toString());
    Injector injector = AppFabricTestHelper.getInjector(cConf);
    discoveryService = injector.getInstance(DiscoveryServiceClient.class);
    appFabricServer = injector.getInstance(AppFabricServer.class);
    appFabricServer.startAndWait();
    waitForService(Constants.Service.APP_FABRIC_HTTP);
    accessEnforcer = injector.getInstance(RemoteAccessEnforcer.class);
    authorizationEnforcer = new AccessEnforcerWrapper(accessEnforcer);
    privilegesManager = injector.getInstance(PrivilegesManager.class);
  }

  private static void waitForService(String name) {
    EndpointStrategy endpointStrategy = new RandomEndpointStrategy(() -> discoveryService.discover(name));
    Preconditions.checkNotNull(endpointStrategy.pick(5, TimeUnit.SECONDS),
                               "%s service is not up after 5 seconds", name);
  }

  @After
  public void after() throws Exception {
    ((RemoteAccessEnforcer) accessEnforcer).clearCache();
  }

  @Test
  public void testPrivilegesManager() throws Exception {
    // In this test, grants and revokes happen via PrivilegesManager, privilege listing and enforcement happens via
    // Authorizer. Also, since grants and revokes go directly to master and don't need a proxy, the
    // RemoteSystemOperationsService does not need to be started in this release.
    privilegesManager.grant(Authorizable.fromEntityId(NS), ALICE, EnumSet.allOf(Action.class));
    privilegesManager.grant(Authorizable.fromEntityId(APP), ALICE, Collections.singleton(Action.ADMIN));
    privilegesManager.grant(Authorizable.fromEntityId(PROGRAM), ALICE, Collections.singleton(Action.EXECUTE));
    authorizationEnforcer.enforce(NS, ALICE, EnumSet.allOf(Action.class));
    authorizationEnforcer.enforce(APP, ALICE, Action.ADMIN);
    authorizationEnforcer.enforce(PROGRAM, ALICE, Action.EXECUTE);
    authorizationEnforcer.enforce(APP, ALICE, Collections.singleton(Action.ADMIN));
    privilegesManager.revoke(Authorizable.fromEntityId(PROGRAM));
    privilegesManager.revoke(Authorizable.fromEntityId(APP), ALICE, EnumSet.allOf(Action.class));
    privilegesManager.revoke(Authorizable.fromEntityId(NS), ALICE, EnumSet.allOf(Action.class));
    Set<Privilege> privileges = privilegesManager.listPrivileges(ALICE);
    Assert.assertTrue(String.format("Expected all of alice's privileges to be revoked, but found %s", privileges),
                      privileges.isEmpty());
  }

  @Test
  public void testAuthorizationEnforcer() throws Exception {
    privilegesManager.grant(Authorizable.fromEntityId(NS), ALICE, EnumSet.allOf(Action.class));
    privilegesManager.grant(Authorizable.fromEntityId(APP), ALICE, Collections.singleton(Action.ADMIN));
    privilegesManager.grant(Authorizable.fromEntityId(PROGRAM), ALICE, Collections.singleton(Action.EXECUTE));
    authorizationEnforcer.enforce(NS, ALICE, EnumSet.allOf(Action.class));
    authorizationEnforcer.enforce(APP, ALICE, Action.ADMIN);
    authorizationEnforcer.enforce(PROGRAM, ALICE, Action.EXECUTE);
    try {
      authorizationEnforcer.enforce(NS, BOB, Action.ADMIN);
      Assert.fail();
    } catch (UnauthorizedException e) {
      // expected
    }

    privilegesManager.revoke(Authorizable.fromEntityId(PROGRAM));
    privilegesManager.revoke(Authorizable.fromEntityId(APP));
    privilegesManager.revoke(Authorizable.fromEntityId(NS));
  }

  @Test
  public void testVisibility() throws Exception {
    ApplicationId app1 = NS.app("app1");
    ProgramId program1 = app1.program(ProgramType.SERVICE, "service1");

    ApplicationId app2 = NS.app("app2");
    ProgramId program2 = app2.program(ProgramType.MAPREDUCE, "service2");

    DatasetId ds = NS.dataset("ds");
    DatasetId ds1 = NS.dataset("ds1");
    DatasetId ds2 = NS.dataset("ds2");

    // Grant privileges on non-numbered entities to ALICE
    privilegesManager.grant(Authorizable.fromEntityId(PROGRAM), ALICE, Collections.singleton(Action.EXECUTE));
    privilegesManager.grant(Authorizable.fromEntityId(ds), ALICE, EnumSet.of(Action.READ, Action.WRITE));

    // Grant privileges on entities ending with 2 to BOB
    privilegesManager.grant(Authorizable.fromEntityId(program2), BOB, Collections.singleton(Action.ADMIN));
    privilegesManager.grant(Authorizable.fromEntityId(ds2), BOB, EnumSet.of(Action.READ, Action.WRITE));

    Set<? extends EntityId> allEntities = ImmutableSet.of(NS,
                                                          APP, PROGRAM, ds,
                                                          app1, program1, ds1,
                                                          app2, program2, ds2);

    Assert.assertEquals(ImmutableSet.of(NS, APP, PROGRAM, ds),
                        authorizationEnforcer.isVisible(allEntities, ALICE));

    Assert.assertEquals(ImmutableSet.of(NS, app2, program2, ds2),
                        authorizationEnforcer.isVisible(allEntities, BOB));

    Assert.assertEquals(ImmutableSet.of(),
                        authorizationEnforcer.isVisible(allEntities, CAROL));

    Assert.assertEquals(ImmutableSet.of(),
                        authorizationEnforcer.isVisible(ImmutableSet.<EntityId>of(), ALICE));

    Assert.assertEquals(ImmutableSet.of(ds, APP),
                        authorizationEnforcer.isVisible(ImmutableSet.of(ds, APP), ALICE));

    for (EntityId entityId : allEntities) {
      privilegesManager.revoke(Authorizable.fromEntityId(entityId));
    }
  }

  @Test
  public void testSingleVisibility() throws Exception {
    ApplicationId app1 = NS.app("app1");
    ProgramId program1 = app1.program(ProgramType.SERVICE, "service1");

    ApplicationId app2 = NS.app("app2");
    ProgramId program2 = app2.program(ProgramType.MAPREDUCE, "service2");

    DatasetId ds = NS.dataset("ds");
    DatasetId ds1 = NS.dataset("ds1");
    DatasetId ds2 = NS.dataset("ds2");

    // Grant privileges on non-numbered entities to ALICE
    privilegesManager.grant(Authorizable.fromEntityId(PROGRAM), ALICE, Collections.singleton(Action.EXECUTE));
    privilegesManager.grant(Authorizable.fromEntityId(ds), ALICE, EnumSet.of(Action.READ, Action.WRITE));

    // Grant privileges on entities ending with 2 to BOB
    privilegesManager.grant(Authorizable.fromEntityId(program2), BOB, Collections.singleton(Action.ADMIN));
    privilegesManager.grant(Authorizable.fromEntityId(ds2), BOB, EnumSet.of(Action.READ, Action.WRITE));

    Set<? extends EntityId> allEntities = ImmutableSet.of(NS,
                                                          APP, PROGRAM, ds,
                                                          app1, program1, ds1,
                                                          app2, program2, ds2);

    // Verify ALICE has the right permissions
    authorizationEnforcer.isVisible(PROGRAM, ALICE);
    authorizationEnforcer.isVisible(ds, ALICE);
    shouldNotHaveVisibility(authorizationEnforcer, program2, ALICE);
    shouldNotHaveVisibility(authorizationEnforcer, ds2, ALICE);

    // Verify BOB has the right permissions
    authorizationEnforcer.isVisible(program2, BOB);
    authorizationEnforcer.isVisible(ds2, BOB);
    shouldNotHaveVisibility(authorizationEnforcer, PROGRAM, BOB);
    shouldNotHaveVisibility(authorizationEnforcer, ds, BOB);

    // Verify CAROL has the right permissions
    shouldNotHaveVisibility(authorizationEnforcer, PROGRAM, CAROL);
    shouldNotHaveVisibility(authorizationEnforcer, ds, CAROL);
    shouldNotHaveVisibility(authorizationEnforcer, program2, CAROL);
    shouldNotHaveVisibility(authorizationEnforcer, ds2, CAROL);

    for (EntityId entityId : allEntities) {
      privilegesManager.revoke(Authorizable.fromEntityId(entityId));
    }
  }

  // Throws an assertion failure exception if the visibility check succeeds.
  protected void shouldNotHaveVisibility(AuthorizationEnforcer enforcer, EntityId entityId, Principal principal)
    throws Exception {
    try {
      enforcer.isVisible(entityId, principal);
      Assert.fail(String.format("Expected principal %s to not have visibility permissions for entity '%s'.", principal,
                                entityId));
    } catch (UnauthorizedException e) {
      // This is expected.
    }
  }

  @AfterClass
  public static void tearDown() {
    appFabricServer.stopAndWait();
    AppFabricTestHelper.shutdown();
  }
}
