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

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

/**
 * Test to verify Route Config.
 */
public class RouteConfigTest {

  @Test
  public void testValidRouteConfig() {
    Map<String, Integer> configDist = ImmutableMap.<String, Integer>builder()
      .put("v1", 20).put("v2", 30).put("v3", 50).build();
    RouteConfig config = new RouteConfig(configDist);
    Assert.assertEquals(configDist, config.getRoutes());
    configDist = ImmutableMap.<String, Integer>builder().put("v2", 100).build();
    config = new RouteConfig(configDist);
    Assert.assertEquals(configDist, config.getRoutes());
  }

  @Test
  public void testInvalidRouteConfig() {
    Map<String, Integer> configDist = ImmutableMap.<String, Integer>builder().put("v1", 20).put("v2", 50).build();
    try {
      RouteConfig config = new RouteConfig(configDist);
      Assert.fail("RouteConfig creation should have failed since the values didn't add up to 100.");
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      RouteConfig config = new RouteConfig(Collections.<String, Integer>emptyMap());
      Assert.fail("RouteConfig creation should have failed since the values didn't add up to 100.");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
