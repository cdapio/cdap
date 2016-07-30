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

package co.cask.cdap.internal.app.store.remote;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.gateway.handlers.meta.RemoteSystemOperationsService;
import co.cask.cdap.internal.app.services.AppFabricServer;
import co.cask.cdap.internal.guice.AppFabricTestModule;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.cdap.security.authorization.InMemoryAuthorizer;
import co.cask.cdap.security.spi.authorization.PrivilegesFetcher;
import co.cask.tephra.TransactionManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
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
import java.util.concurrent.TimeUnit;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Tests for RemotePrivilegesFetcher. These are in app-fabric, because we need to start app-fabric in these tests.
 */
public class RemotePrivilegesFetcherTest {
  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static AuthorizerInstantiator authorizerInstantiator;
  private static PrivilegesFetcher privilegesFetcher;
  private static TransactionManager txManager;
  private static DatasetService datasetService;
  private static AppFabricServer appFabricServer;
  private static RemoteSystemOperationsService remoteSysOpService;

  @BeforeClass
  public static void setup() throws IOException, InterruptedException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMPORARY_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.KERBEROS_ENABLED, false);
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, InMemoryAuthorizer.class.getName());
    LocationFactory locationFactory = new LocalLocationFactory(TEMPORARY_FOLDER.newFolder());
    Location externalAuthJar = AppJarHelper.createDeploymentJar(locationFactory, InMemoryAuthorizer.class, manifest);
    cConf.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, externalAuthJar.toString());
    Injector injector = Guice.createInjector(new AppFabricTestModule(cConf));
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();
    appFabricServer = injector.getInstance(AppFabricServer.class);
    appFabricServer.startAndWait();
    remoteSysOpService = injector.getInstance(RemoteSystemOperationsService.class);
    remoteSysOpService.startAndWait();
    waitForServices(injector.getInstance(DiscoveryServiceClient.class));
    authorizerInstantiator = injector.getInstance(AuthorizerInstantiator.class);
    privilegesFetcher = injector.getInstance(PrivilegesFetcher.class);
  }

  private static void waitForServices(DiscoveryServiceClient discoveryService) throws InterruptedException {
    waitForService(discoveryService, Constants.Service.DATASET_MANAGER);
    waitForService(discoveryService, Constants.Service.APP_FABRIC_HTTP);
    waitForService(discoveryService, Constants.Service.REMOTE_SYSTEM_OPERATION);
  }

  private static void waitForService(DiscoveryServiceClient discoveryService, String name) throws InterruptedException {
    EndpointStrategy endpointStrategy = new RandomEndpointStrategy(discoveryService.discover(name));
    Preconditions.checkNotNull(endpointStrategy.pick(5, TimeUnit.SECONDS),
                               "%s service is not up after 5 seconds", name);
  }

  @Test
  public void test() throws Exception {
    Principal alice = new Principal("alice", Principal.PrincipalType.USER);
    NamespaceId ns = new NamespaceId("ns");
    ApplicationId app = ns.app("app");
    ProgramId program = app.program(ProgramType.FLOW, "flo");
    authorizerInstantiator.get().grant(ns, alice, ImmutableSet.of(Action.WRITE));
    authorizerInstantiator.get().grant(app, alice, ImmutableSet.of(Action.ADMIN));
    authorizerInstantiator.get().grant(program, alice, ImmutableSet.of(Action.EXECUTE));
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(ns, Action.WRITE),
        new Privilege(app, Action.ADMIN),
        new Privilege(program, Action.EXECUTE)
      ),
      privilegesFetcher.listPrivileges(alice));
  }

  @AfterClass
  public static void tearDown() {
    remoteSysOpService.stopAndWait();
    appFabricServer.stopAndWait();
    datasetService.stopAndWait();
    txManager.stopAndWait();
  }
}
