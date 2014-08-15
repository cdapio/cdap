/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.test.internal.AppFabricTestHelper;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class AppFabricServerTest {
  private static AppFabricServer server;
  private static DiscoveryServiceClient discoveryServiceClient;

  @BeforeClass
  public static void before() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector();
    server = injector.getInstance(AppFabricServer.class);
    discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
  }

  @Test
  public void startStopServer() throws Exception {
    Service.State state = server.startAndWait();
    Assert.assertTrue(state == Service.State.RUNNING);
    // Sleep shortly for the discovery
    TimeUnit.SECONDS.sleep(1);
    ServiceDiscovered discovered = discoveryServiceClient.discover(Constants.Service.APP_FABRIC_HTTP);
    Assert.assertFalse(Iterables.isEmpty(discovered));

    state = server.stopAndWait();
    Assert.assertTrue(state == Service.State.TERMINATED);

    TimeUnit.SECONDS.sleep(1);
    Assert.assertTrue(Iterables.isEmpty(discovered));
  }
}
