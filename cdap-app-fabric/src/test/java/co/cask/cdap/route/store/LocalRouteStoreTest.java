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
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

/**
 * Route Configuration Store Test.
 */
public class LocalRouteStoreTest extends AppFabricTestBase {

  @Test
  public void testRouteStorage() throws Exception {
    RouteStore routeStore = getInjector().getInstance(RouteStore.class);
    ApplicationId appId = new ApplicationId("n1", "a1");
    ProgramId service1 = appId.service("s1");
    RouteConfig routeConfig = new RouteConfig(ImmutableMap.of("v1", 100));
    routeStore.store(service1, routeConfig);
    Assert.assertEquals(routeConfig.getRoutes(), routeStore.fetch(service1).getRoutes());
    routeStore.delete(service1);
    Assert.assertNotNull(routeStore.fetch(service1));

    try {
      routeStore.delete(service1);
      Assert.fail("Config should have been deleted and thus a NotFoundException must have been thrown.");
    } catch (NotFoundException e) {
      // expected
    }
  }
}
