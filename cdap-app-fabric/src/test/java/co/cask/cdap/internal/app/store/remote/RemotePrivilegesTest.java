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
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.AuthorizationPrivilege;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.security.authorization.InMemoryAuthorizer;
import co.cask.cdap.security.authorization.RemoteAuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import com.google.common.base.Preconditions;
import com.google.inject.Injector;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Tests for remote implementations of {@link AuthorizationEnforcer} and {@link PrivilegesManager}.
 * These are in app-fabric, because we need to start app-fabric in these tests.
 */
public class RemotePrivilegesTest {
  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final Principal ALICE = new Principal("alice", Principal.PrincipalType.USER);
  private static final Principal BOB = new Principal("bob", Principal.PrincipalType.USER);
  private static final NamespaceId NS = new NamespaceId("ns");
  private static final ApplicationId APP = NS.app("app");
  private static final ProgramId PROGRAM = APP.program(ProgramType.FLOW, "flo");
  private static final int CACHE_TIMEOUT = 3;

  private static AuthorizationEnforcer authorizationEnforcer;
  private static PrivilegesManager privilegesManager;
  private static DiscoveryServiceClient discoveryService;
  private static AppFabricServer appFabricServer;

  @BeforeClass
  public static void setup() throws IOException, InterruptedException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMPORARY_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.KERBEROS_ENABLED, false);
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
    cConf.setInt(Constants.Security.Authorization.CACHE_MAX_ENTRIES, 10000);
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

  @Test
  public void testPrivilegesManager() throws Exception {
    // In this test, grants and revokes happen via PrivilegesManager, privilege listing and enforcement happens via
    // Authorizer. Also, since grants and revokes go directly to master and don't need a proxy, the
    // RemoteSystemOperationsService does not need to be started in this release.
    privilegesManager.grant(NS, ALICE, EnumSet.allOf(Action.class));
    privilegesManager.grant(APP, ALICE, Collections.singleton(Action.ADMIN));
    privilegesManager.grant(PROGRAM, ALICE, Collections.singleton(Action.EXECUTE));
    authorizationEnforcer.enforce(NS, ALICE, EnumSet.allOf(Action.class));
    authorizationEnforcer.enforce(APP, ALICE, Action.ADMIN);
    authorizationEnforcer.enforce(PROGRAM, ALICE, Action.EXECUTE);
    authorizationEnforcer.enforce(APP, ALICE, EnumSet.allOf(Action.class));
    privilegesManager.revoke(PROGRAM);
    privilegesManager.revoke(APP, ALICE, EnumSet.allOf(Action.class));
    privilegesManager.revoke(NS, ALICE, EnumSet.allOf(Action.class));
    Set<Privilege> privileges = privilegesManager.listPrivileges(ALICE);
    Assert.assertTrue(String.format("Expected all of alice's privileges to be revoked, but found %s", privileges),
                      privileges.isEmpty());
  }

  @Test
  public void testAuthorizationEnforcer() throws Exception {
    int currentCount = ((RemoteAuthorizationEnforcer) authorizationEnforcer).cacheAsMap().size();
    privilegesManager.grant(NS, ALICE, EnumSet.allOf(Action.class));
    privilegesManager.grant(APP, ALICE, Collections.singleton(Action.ADMIN));
    privilegesManager.grant(PROGRAM, ALICE, Collections.singleton(Action.EXECUTE));
    authorizationEnforcer.enforce(NS, ALICE, EnumSet.allOf(Action.class));
    authorizationEnforcer.enforce(APP, ALICE, Action.ADMIN);
    authorizationEnforcer.enforce(PROGRAM, ALICE, Action.EXECUTE);
    Map<AuthorizationPrivilege, Boolean> authorizationPrivilegeBooleanMap =
      ((RemoteAuthorizationEnforcer) authorizationEnforcer).cacheAsMap();
    // 4 For EnumSet.allOf(Action.class) and one each for the next 2
    Assert.assertEquals(currentCount + 6, authorizationPrivilegeBooleanMap.size());
    Assert.assertTrue(authorizationPrivilegeBooleanMap.containsKey(
      new AuthorizationPrivilege(ALICE, NS, Action.ADMIN)));
    Assert.assertTrue(authorizationPrivilegeBooleanMap.containsKey(
      new AuthorizationPrivilege(ALICE, NS, Action.READ)));
    Assert.assertTrue(authorizationPrivilegeBooleanMap.containsKey(
      new AuthorizationPrivilege(ALICE, NS, Action.WRITE)));
    Assert.assertTrue(authorizationPrivilegeBooleanMap.containsKey(
      new AuthorizationPrivilege(ALICE, NS, Action.EXECUTE)));
    Assert.assertFalse(authorizationPrivilegeBooleanMap.containsKey(
      new AuthorizationPrivilege(BOB, NS, Action.ADMIN)));
    Assert.assertFalse(authorizationPrivilegeBooleanMap.containsKey(
      new AuthorizationPrivilege(BOB, NS, Action.READ)));
    Assert.assertFalse(authorizationPrivilegeBooleanMap.containsKey(
      new AuthorizationPrivilege(BOB, NS, Action.WRITE)));
    Assert.assertFalse(authorizationPrivilegeBooleanMap.containsKey(
      new AuthorizationPrivilege(BOB, NS, Action.EXECUTE)));
    privilegesManager.grant(NS, BOB, Collections.singleton(Action.ADMIN));
    authorizationEnforcer.enforce(NS, BOB, Action.ADMIN);
    Assert.assertTrue(authorizationPrivilegeBooleanMap.containsKey(
      new AuthorizationPrivilege(BOB, NS, Action.ADMIN)));
    TimeUnit.SECONDS.sleep(CACHE_TIMEOUT);
    Assert.assertFalse(authorizationPrivilegeBooleanMap.containsKey(
      new AuthorizationPrivilege(ALICE, NS, Action.ADMIN)));
    Assert.assertFalse(authorizationPrivilegeBooleanMap.containsKey(
      new AuthorizationPrivilege(ALICE, NS, Action.READ)));
    Assert.assertFalse(authorizationPrivilegeBooleanMap.containsKey(
      new AuthorizationPrivilege(ALICE, NS, Action.WRITE)));
  }

  @AfterClass
  public static void tearDown() {
    appFabricServer.stopAndWait();
  }
}
