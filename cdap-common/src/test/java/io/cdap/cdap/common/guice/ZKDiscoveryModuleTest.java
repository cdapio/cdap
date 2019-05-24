/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.common.guice;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.service.ServiceDiscoverable;
import io.cdap.cdap.common.twill.TwillAppNames;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClients;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for the {@link ZKDiscoveryModule}.
 */
public class ZKDiscoveryModuleTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static InMemoryZKServer zkServer;
  private static CConfiguration cConf;

  @BeforeClass
  public static void init() throws IOException {
    zkServer = InMemoryZKServer.builder().setDataDir(TEMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();

    cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
  }

  @AfterClass
  public static void finish() {
    zkServer.stopAndWait();
  }

  @Test
  public void testMasterDiscovery() {
    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new ZKClientModule(),
      new ZKDiscoveryModule()
    );

    ZKClientService zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAndWait();
    try {
      DiscoveryService discoveryService = injector.getInstance(DiscoveryService.class);
      DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);

      // Register a master service
      InetSocketAddress socketAddr = new InetSocketAddress(InetAddress.getLoopbackAddress(), 43210);
      Cancellable cancellable = discoveryService.register(new Discoverable(Constants.Service.APP_FABRIC_HTTP,
                                                                           socketAddr));

      try {
        // Discover the master service
        Discoverable discoverable = new RandomEndpointStrategy(
          () -> discoveryServiceClient.discover(Constants.Service.APP_FABRIC_HTTP)).pick(10, TimeUnit.SECONDS);

        Assert.assertNotNull(discoverable);
        Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, discoverable.getName());
        Assert.assertEquals(socketAddr, discoverable.getSocketAddress());
      } finally {
        cancellable.cancel();
      }

    } finally {
      zkClient.stopAndWait();
    }
  }

  @Test
  public void testProgramDiscovery() {
    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new ZKClientModule(),
      new ZKDiscoveryModule()
    );

    ZKClientService zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAndWait();
    try {
      // Register a service using the twill ZKClient. This is to simulate how a user Service program register
      ProgramId programId = NamespaceId.DEFAULT.app("app").service("service");
      String twillNamespace = injector.getInstance(CConfiguration.class).get(Constants.CFG_TWILL_ZK_NAMESPACE);

      String discoverableName = ServiceDiscoverable.getName(programId);
      ZKClient twillZKClient = ZKClients.namespace(zkClient,
                                                   twillNamespace + "/" + TwillAppNames.toTwillAppName(programId));

      try (ZKDiscoveryService twillDiscoveryService = new ZKDiscoveryService(twillZKClient)) {
        InetSocketAddress socketAddr = new InetSocketAddress(InetAddress.getLoopbackAddress(), 43210);
        Cancellable cancellable = twillDiscoveryService.register(new Discoverable(discoverableName, socketAddr));
        try {
          // Discover the user service
          DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
          Discoverable discoverable = new RandomEndpointStrategy(
            () -> discoveryServiceClient.discover(discoverableName)).pick(10, TimeUnit.SECONDS);

          Assert.assertNotNull(discoverable);

        } finally {
          cancellable.cancel();
        }
      }
    } finally {
      zkClient.stopAndWait();
    }
  }
}
