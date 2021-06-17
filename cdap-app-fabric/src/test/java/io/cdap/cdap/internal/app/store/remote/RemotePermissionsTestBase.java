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
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.services.AppFabricServer;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.security.ApplicationPermission;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.GrantedPermission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.authorization.RemoteAccessEnforcer;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.PermissionManager;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import org.apache.twill.discovery.DiscoveryServiceClient;
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

/**
 * Tests for remote implementations of {@link AccessEnforcer} and {@link PermissionManager}.
 * These are in app-fabric, because we need to start app-fabric in these tests.
 */
@SuppressWarnings("WeakerAccess")
public abstract class RemotePermissionsTestBase {
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
  protected static PermissionManager permissionManager;
  protected static CConfiguration cConf = CConfiguration.create();

  private static DiscoveryServiceClient discoveryService;
  private static AppFabricServer appFabricServer;

  protected static void setup() throws IOException, InterruptedException {
    AppFabricTestHelper.enableAuthorization(cConf, TEMPORARY_FOLDER);
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMPORARY_FOLDER.newFolder().getAbsolutePath());
    cConf.setInt(Constants.Security.Authorization.CACHE_TTL_SECS, CACHE_TIMEOUT);
    Injector injector = AppFabricTestHelper.getInjector(cConf);
    discoveryService = injector.getInstance(DiscoveryServiceClient.class);
    appFabricServer = injector.getInstance(AppFabricServer.class);
    appFabricServer.startAndWait();
    waitForService(Constants.Service.APP_FABRIC_HTTP);
    accessEnforcer = injector.getInstance(RemoteAccessEnforcer.class);
    permissionManager = injector.getInstance(PermissionManager.class);
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
  public void testPermissionManager() throws Exception {
    // In this test, grants and revokes happen via PermissionManager, privilege listing and enforcement happens via
    // AccessEnforcer. Also, since grants and revokes go directly to master and don't need a proxy, the
    // RemoteSystemOperationsService does not need to be started in this release.
    permissionManager.grant(Authorizable.fromEntityId(NS), ALICE, EnumSet.allOf(StandardPermission.class));
    permissionManager.grant(Authorizable.fromEntityId(APP), ALICE, Collections.singleton(StandardPermission.UPDATE));
    permissionManager.grant(Authorizable.fromEntityId(PROGRAM), ALICE,
                            Collections.singleton(ApplicationPermission.EXECUTE));
    permissionManager.grant(Authorizable.fromEntityId(NS, EntityType.PROFILE), ALICE,
                            Collections.singleton(StandardPermission.LIST));
    accessEnforcer.enforce(NS, ALICE, EnumSet.allOf(StandardPermission.class));
    accessEnforcer.enforce(APP, ALICE, StandardPermission.UPDATE);
    accessEnforcer.enforce(PROGRAM, ALICE, ApplicationPermission.EXECUTE);
    accessEnforcer.enforce(APP, ALICE, Collections.singleton(StandardPermission.UPDATE));
    accessEnforcer.enforceOnParent(EntityType.PROFILE, NS, ALICE, StandardPermission.LIST);
    permissionManager.revoke(Authorizable.fromEntityId(PROGRAM));
    permissionManager.revoke(Authorizable.fromEntityId(APP), ALICE, EnumSet.allOf(StandardPermission.class));
    permissionManager.revoke(Authorizable.fromEntityId(NS), ALICE, EnumSet.allOf(StandardPermission.class));
    permissionManager.revoke(Authorizable.fromEntityId(NS, EntityType.PROFILE), ALICE,
                            Collections.singleton(StandardPermission.LIST));
    Set<GrantedPermission> permissions = permissionManager.listGrants(ALICE);
    Assert.assertTrue(String.format("Expected all of alice's permissions to be revoked, but found %s", permissions),
                      permissions.isEmpty());
  }

  @Test
  public void testAccessEnforcer() throws Exception {
    permissionManager.grant(Authorizable.fromEntityId(NS), ALICE, EnumSet.allOf(StandardPermission.class));
    permissionManager.grant(Authorizable.fromEntityId(APP), ALICE, Collections.singleton(StandardPermission.UPDATE));
    permissionManager.grant(Authorizable.fromEntityId(PROGRAM), ALICE,
                            Collections.singleton(ApplicationPermission.EXECUTE));
    permissionManager.grant(Authorizable.fromEntityId(NS, EntityType.PROFILE), ALICE,
                            Collections.singleton(StandardPermission.LIST));
    accessEnforcer.enforce(NS, ALICE, EnumSet.allOf(StandardPermission.class));
    accessEnforcer.enforce(APP, ALICE, StandardPermission.UPDATE);
    accessEnforcer.enforce(PROGRAM, ALICE, ApplicationPermission.EXECUTE);
    accessEnforcer.enforceOnParent(EntityType.PROFILE, NS, ALICE, StandardPermission.LIST);
    assertUnauthorized(() -> accessEnforcer.enforce(NS, BOB, StandardPermission.UPDATE));
    assertUnauthorized(() -> accessEnforcer.enforceOnParent(EntityType.PROFILE, NS, BOB, StandardPermission.LIST));
    assertUnauthorized(() -> accessEnforcer.enforceOnParent(EntityType.PROFILE, NS, ALICE, StandardPermission.CREATE));
    assertUnauthorized(() -> accessEnforcer.enforceOnParent(EntityType.APPLICATION, NS, ALICE,
                                                            StandardPermission.LIST));

    permissionManager.revoke(Authorizable.fromEntityId(PROGRAM));
    permissionManager.revoke(Authorizable.fromEntityId(APP));
    permissionManager.revoke(Authorizable.fromEntityId(NS));
    permissionManager.revoke(Authorizable.fromEntityId(NS, EntityType.PROFILE));
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

    // Grant permissions on non-numbered entities to ALICE
    permissionManager.grant(Authorizable.fromEntityId(PROGRAM), ALICE,
                            Collections.singleton(ApplicationPermission.EXECUTE));
    permissionManager.grant(Authorizable.fromEntityId(ds), ALICE,
                            EnumSet.of(StandardPermission.GET, StandardPermission.UPDATE));

    // Grant permissions on entities ending with 2 to BOB
    permissionManager.grant(Authorizable.fromEntityId(program2), BOB,
                            Collections.singleton(StandardPermission.UPDATE));
    permissionManager.grant(Authorizable.fromEntityId(ds2), BOB,
                            EnumSet.of(StandardPermission.GET, StandardPermission.UPDATE));

    Set<? extends EntityId> allEntities = ImmutableSet.of(NS,
                                                          APP, PROGRAM, ds,
                                                          app1, program1, ds1,
                                                          app2, program2, ds2);

    Assert.assertEquals(ImmutableSet.of(NS, APP, PROGRAM, ds),
                        accessEnforcer.isVisible(allEntities, ALICE));

    Assert.assertEquals(ImmutableSet.of(NS, app2, program2, ds2),
                        accessEnforcer.isVisible(allEntities, BOB));

    Assert.assertEquals(ImmutableSet.of(),
                        accessEnforcer.isVisible(allEntities, CAROL));

    Assert.assertEquals(ImmutableSet.of(),
                        accessEnforcer.isVisible(ImmutableSet.<EntityId>of(), ALICE));

    Assert.assertEquals(ImmutableSet.of(ds, APP),
                        accessEnforcer.isVisible(ImmutableSet.of(ds, APP), ALICE));

    for (EntityId entityId : allEntities) {
      permissionManager.revoke(Authorizable.fromEntityId(entityId));
    }
  }

  // Throws an assertion failure exception if the visibility check succeeds.
  protected void shouldNotHaveVisibility(AccessEnforcer enforcer, EntityId entityId, Principal principal)
    throws Exception {
    try {
      enforcer.enforce(entityId, principal, StandardPermission.GET);
      Assert.fail(String.format("Expected principal %s to not have visibility permissions for entity '%s'.", principal,
                                entityId));
    } catch (UnauthorizedException e) {
      // This is expected.
    }
  }

  private void assertUnauthorized(Retries.Runnable<AccessException> runnable) throws AccessException {
    try {
      runnable.run();
      Assert.fail();
    } catch (UnauthorizedException e) {
      // expected
    }
  }

  @AfterClass
  public static void tearDown() {
    appFabricServer.stopAndWait();
    AppFabricTestHelper.shutdown();
  }
}
