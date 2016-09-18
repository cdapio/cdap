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

import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.ZKClientModule;
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

/**
 * Tests for {@link ZKRouteStore}.
 */
public class ZKRouteStoreTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  private static InMemoryZKServer zkServer;
  private static Injector injector;
  private static ZKClientService zkClientService;

  @BeforeClass
  public static void init() throws IOException {
    zkServer = InMemoryZKServer.builder().setDataDir(TMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();

    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());

    injector = Guice.createInjector(new ConfigModule(cConf),
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
    ProgramId s1 = appId.service("s1");
    Map<String, Integer> routeMap = ImmutableMap.<String, Integer>builder().put("v1", 30).put("v2", 70).build();
    try (RouteStore routeStore = new ZKRouteStore(zkClientService)) {
      routeStore.store(s1, new RouteConfig(routeMap));
      Assert.assertEquals(routeMap, routeStore.fetch(s1).getRoutes());
      routeStore.delete(s1);
      try {
        routeStore.fetch(s1);
        Assert.fail("Fetch should have thrown a NotFoundException since the config was deleted.");
      } catch (NotFoundException ex) {
        // expected since the config was deleted
      }
    }
  }
}
