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

package co.cask.cdap.route.store;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link ZKRouteStore}.
 */
public class ZKRouteStoreTest {

  private static final Map<String, Integer> TEST_ROUTE_CONFIG = ImmutableMap.<String, Integer>builder()
    .put("v1", 30).put("v2", 70).build();
  private static final Map<String, Integer> TEST_ROUTE_CONFIG2 = ImmutableMap.<String, Integer>builder()
    .put("v1", 70).put("v2", 30).build();


  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  private static InMemoryZKServer zkServer;
  private static ZKClientService zkClientService;

  @BeforeClass
  public static void init() throws IOException {
    zkServer = InMemoryZKServer.builder().setDataDir(TMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();

    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());

    Injector injector = Guice.createInjector(new ConfigModule(cConf),
                                             new ZKClientModule());
    zkClientService = injector.getInstance(ZKClientService.class);
    zkClientService.startAndWait();
  }

  @AfterClass
  public static void finish() throws Exception {
    zkClientService.stopAndWait();
    zkServer.stopAndWait();
  }

  @Test
  public void testStore() throws Exception {
    ApplicationId appId = new ApplicationId("n1", "a1");
    final ProgramId s1 = appId.service("testStore");
    try (final RouteStore routeStore = new ZKRouteStore(zkClientService)) {
      routeStore.store(s1, new RouteConfig(TEST_ROUTE_CONFIG));
      Assert.assertEquals(TEST_ROUTE_CONFIG, routeStore.fetch(s1).getRoutes());
      routeStore.delete(s1);

      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return routeStore.fetch(s1).getRoutes().isEmpty();
        }
      }, 5L, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    }
  }

  @Test
  public void testCreateOfConfig() throws Exception {
    // This test first the config is missing and then created
    ApplicationId appId = new ApplicationId("n1", "a1");
    final ProgramId programId = appId.service("testCreateOfConfig");
    try (final RouteStore writeRouteStore = new ZKRouteStore(zkClientService)) {
      try (final RouteStore readRouteStore = new ZKRouteStore(zkClientService)) {
        // First it should be no route
        Assert.assertTrue(readRouteStore.fetch(programId).getRoutes().isEmpty());

        // Write a new route config
        writeRouteStore.store(programId, new RouteConfig(TEST_ROUTE_CONFIG));

        // Fetch it again and it should has the changes
        Tasks.waitFor(TEST_ROUTE_CONFIG, new Callable<Map<String, Integer>>() {
          @Override
          public Map<String, Integer> call() throws Exception {
            return readRouteStore.fetch(programId).getRoutes();
          }
        }, 5L, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      }
    }
  }

  @Test
  public void testAbsenceOfConfig() throws Exception {
    ApplicationId appId = new ApplicationId("n1", "a1");
    final ProgramId programId = appId.service("testAbsenceOfConfig");
    try (final RouteStore routeStore = new ZKRouteStore(zkClientService)) {
      Assert.assertNotNull(routeStore.fetch(programId));
      Assert.assertTrue(routeStore.fetch(programId).getRoutes().isEmpty());
      routeStore.store(programId, new RouteConfig(TEST_ROUTE_CONFIG));
      Tasks.waitFor(false, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return routeStore.fetch(programId).getRoutes().isEmpty();
        }
      }, 5, TimeUnit.SECONDS);
      Assert.assertEquals(TEST_ROUTE_CONFIG, routeStore.fetch(programId).getRoutes());
    }
  }

  @Test
  public void testChangeConfig() throws Exception {
    ApplicationId appId = new ApplicationId("n1", "a1");
    final ProgramId s1 = appId.service("testChangeConfig");

    // Create two RouteStore, one for writing, one for reading
    try (RouteStore writingRouteStore = new ZKRouteStore(zkClientService)) {
      try (final RouteStore readingRouteStore = new ZKRouteStore(zkClientService)) {
        writingRouteStore.store(s1, new RouteConfig(TEST_ROUTE_CONFIG));
        Assert.assertEquals(TEST_ROUTE_CONFIG, readingRouteStore.fetch(s1).getRoutes());
        writingRouteStore.store(s1, new RouteConfig(TEST_ROUTE_CONFIG2));

        Tasks.waitFor(TEST_ROUTE_CONFIG2, new Callable<Map<String, Integer>>() {
          @Override
          public Map<String, Integer> call() throws Exception {
            return readingRouteStore.fetch(s1).getRoutes();
          }
        }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      }
    }
  }
}
