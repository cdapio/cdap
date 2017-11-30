/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.store.remote;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.services.AppFabricServer;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Authorizable;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.security.authorization.InMemoryAuthorizer;
import co.cask.cdap.security.authorization.RemoteAuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
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
  protected static final ProgramId PROGRAM = APP.program(ProgramType.FLOW, "flo");
  private static final int CACHE_TIMEOUT = 3;

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
    authorizationEnforcer = injector.getInstance(RemoteAuthorizationEnforcer.class);
    privilegesManager = injector.getInstance(PrivilegesManager.class);
  }

  private static void waitForService(String name) throws InterruptedException {
    EndpointStrategy endpointStrategy = new RandomEndpointStrategy(discoveryService.discover(name));
    Preconditions.checkNotNull(endpointStrategy.pick(5, TimeUnit.SECONDS),
                               "%s service is not up after 5 seconds", name);
  }

  @After
  public void after() throws Exception {
    ((RemoteAuthorizationEnforcer) authorizationEnforcer).clearCache();
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

    StreamId stream = NS.stream("stream");
    StreamId stream1 = NS.stream("stream1");
    StreamId stream2 = NS.stream("stream2");

    // Grant privileges on non-numbered entities to ALICE
    privilegesManager.grant(Authorizable.fromEntityId(PROGRAM), ALICE, Collections.singleton(Action.EXECUTE));
    privilegesManager.grant(Authorizable.fromEntityId(ds), ALICE, EnumSet.of(Action.READ, Action.WRITE));
    privilegesManager.grant(Authorizable.fromEntityId(stream), ALICE, EnumSet.of(Action.READ));

    // Grant privileges on entities ending with 2 to BOB
    privilegesManager.grant(Authorizable.fromEntityId(program2), BOB, Collections.singleton(Action.ADMIN));
    privilegesManager.grant(Authorizable.fromEntityId(ds2), BOB, EnumSet.of(Action.READ, Action.WRITE));
    privilegesManager.grant(Authorizable.fromEntityId(stream2), BOB, EnumSet.allOf(Action.class));

    Set<? extends EntityId> allEntities = ImmutableSet.of(NS,
                                                          APP, PROGRAM, ds, stream,
                                                          app1, program1, ds1, stream1,
                                                          app2, program2, ds2, stream2);

    Assert.assertEquals(ImmutableSet.of(NS, APP, PROGRAM, ds, stream),
                        authorizationEnforcer.isVisible(allEntities, ALICE));

    Assert.assertEquals(ImmutableSet.of(NS, app2, program2, ds2, stream2),
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

  @AfterClass
  public static void tearDown() {
    appFabricServer.stopAndWait();
  }
}
