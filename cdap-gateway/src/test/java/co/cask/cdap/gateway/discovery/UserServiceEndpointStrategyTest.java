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

package co.cask.cdap.gateway.discovery;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.service.ServiceDiscoverable;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.route.store.RouteConfig;
import co.cask.cdap.route.store.RouteStore;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * Tests for {@link UserServiceEndpointStrategy}
 */
public class UserServiceEndpointStrategyTest {

  @Test
  public void testVersionStrategy() throws Exception {
    ProgramId serviceId = new ApplicationId("n1", "a1").service("s1");
    String discoverableName = ServiceDiscoverable.getName(serviceId);
    List<Discoverable> candidates = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      candidates.add(new Discoverable(discoverableName, null, Bytes.toBytes(Integer.toString(i))));
    }
    SimpleServiceDiscovered serviceDiscovered = new SimpleServiceDiscovered(candidates);
    RouteStore configStore = new InMemoryRouteStore(Collections.<ProgramId, RouteConfig>emptyMap());
    versionStrategyCheck(serviceDiscovered, configStore, serviceId, false);
    candidates.remove(2);
    versionStrategyCheck(serviceDiscovered, configStore, serviceId, true);
  }

  private void versionStrategyCheck(ServiceDiscovered serviceDiscovered, RouteStore configStore, ProgramId serviceId,
                                    boolean expectedNull) {
    for (RouteFallbackStrategy fallbackStrategy : RouteFallbackStrategy.values()) {
      UserServiceEndpointStrategy strategy = new UserServiceEndpointStrategy(
        serviceDiscovered, configStore, serviceId,
        RouteFallbackStrategy.valueOfRouteFallbackStrategy(fallbackStrategy.name()), "2");
      for (int i = 0; i < 100; i++) {
        Discoverable picked = strategy.pick();
        if (!expectedNull) {
          Assert.assertEquals("2", Bytes.toString(picked.getPayload()));
        } else {
          Assert.assertNull(picked);
        }
      }
    }
  }

  @Test
  public void testFallback() throws Exception {
    ProgramId serviceId = new ApplicationId("n1", "a1").service("s1");
    String discoverableName = ServiceDiscoverable.getName(serviceId);
    List<Discoverable> candidates = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      candidates.add(new Discoverable(discoverableName, null, Bytes.toBytes(Integer.toString(i))));
    }
    SimpleServiceDiscovered serviceDiscovered = new SimpleServiceDiscovered(candidates);
    Map<ProgramId, RouteConfig> routeConfigMap = new HashMap<>();
    routeConfigMap.put(serviceId, new RouteConfig(Collections.<String, Integer>emptyMap()));
    RouteStore configStore = new InMemoryRouteStore(routeConfigMap);
    UserServiceEndpointStrategy strategy = new UserServiceEndpointStrategy(serviceDiscovered, configStore, serviceId,
                                                                           RouteFallbackStrategy.SMALLEST, null);
    for (int i = 0; i < 1000; i++) {
      Discoverable picked = strategy.pick();
      Assert.assertEquals("0", Bytes.toString(picked.getPayload()));
    }

    // Remove "0", so the smallest version should now be "1"
    candidates.remove(0);
    for (int i = 0; i < 1000; i++) {
      Discoverable picked = strategy.pick();
      Assert.assertEquals("1", Bytes.toString(picked.getPayload()));
    }

    // Test greatest strategy
    strategy = new UserServiceEndpointStrategy(serviceDiscovered, configStore, serviceId,
                                               RouteFallbackStrategy.LARGEST, null);
    for (int i = 0; i < 1000; i++) {
      Discoverable picked = strategy.pick();
      Assert.assertEquals("4", Bytes.toString(picked.getPayload()));
    }

    // Remove "4", so the largest version should now be "3"
    candidates.remove(candidates.size() - 1);
    for (int i = 0; i < 1000; i++) {
      Discoverable picked = strategy.pick();
      Assert.assertEquals("3", Bytes.toString(picked.getPayload()));
    }

    // Test random strategy - remaining versions are 1, 2, 3
    strategy = new UserServiceEndpointStrategy(serviceDiscovered, configStore, serviceId,
                                               RouteFallbackStrategy.RANDOM, null);
    Set<String> pickedVersions = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      Discoverable picked = strategy.pick();
      pickedVersions.add(Bytes.toString(picked.getPayload()));
    }
    // There is a good probability that more than one version has been picked since its random
    Assert.assertTrue(pickedVersions.size() > 1);

    // Test drop strategy
    strategy = new UserServiceEndpointStrategy(serviceDiscovered, configStore, serviceId,
                                               RouteFallbackStrategy.DROP, null);
    for (int i = 0; i < 1000; i++) {
      Assert.assertNull(strategy.pick());
    }
  }

  @Test
  public void testStrategy() throws Exception {
    ProgramId serviceId = new ApplicationId("n1", "a1").service("s1");
    String discoverableName = ServiceDiscoverable.getName(serviceId);
    List<Discoverable> candidates = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      candidates.add(new Discoverable(discoverableName, null, Bytes.toBytes(Integer.toString(i))));
    }
    SimpleServiceDiscovered serviceDiscovered = new SimpleServiceDiscovered(candidates);
    Map<String, Integer> routeToVersion = ImmutableMap.of("2", 100);
    Map<ProgramId, RouteConfig> routeMap = ImmutableMap.of(serviceId, new RouteConfig(routeToVersion));
    RouteStore configStore = new InMemoryRouteStore(routeMap);
    UserServiceEndpointStrategy strategy = new UserServiceEndpointStrategy(serviceDiscovered, configStore, serviceId);

    for (int i = 0; i < 1000; i++) {
      Discoverable picked = strategy.pick();
      Assert.assertEquals("2", Bytes.toString(picked.getPayload()));
    }

    // Switch config to choose version 3 always
    routeToVersion = ImmutableMap.of("3", 100);
    configStore.store(serviceId, new RouteConfig(routeToVersion));

    for (int i = 0; i < 1000; i++) {
      Discoverable picked = strategy.pick();
      Assert.assertEquals("3", Bytes.toString(picked.getPayload()));
    }

    // Switch config to choose verion 1 and 4 - 50% each
    routeToVersion = ImmutableMap.of("1", 50, "4", 50);
    configStore.store(serviceId, new RouteConfig(routeToVersion));

    Map<String, Integer> resultMap = new HashMap<>();
    for (int i = 0; i < 10000; i++) {
      Discoverable picked = strategy.pick();
      String version = Bytes.toString(picked.getPayload());
      if (resultMap.containsKey(version)) {
        resultMap.put(version, resultMap.get(version) + 1);
      } else {
        resultMap.put(version, 1);
      }
    }
    Assert.assertEquals(2, resultMap.size());
    double requestsToOne = resultMap.get("1");
    double requestsToTwo = resultMap.get("4");
    double requestRatio = requestsToOne / requestsToTwo;
    // Request Ratio should be close to 1.0 since we expect 50% of requests to go to each of these versions
    Assert.assertTrue(String.format("RequestRatio was %f and 1 got %f and 4 got %f",
                                    requestRatio, requestsToOne, requestsToTwo), requestRatio >= 0.7);
    Assert.assertTrue(String.format("RequestRatio was %f and 1 got %f and 4 got %f",
                                    requestRatio, requestsToOne, requestsToTwo), requestRatio <= 1.3);

    // Set the payload filter
    strategy = new UserServiceEndpointStrategy(serviceDiscovered, configStore, serviceId, null, "1");
    for (int i = 0; i < 1000; i++) {
      Discoverable picked = strategy.pick();
      Assert.assertEquals("1", Bytes.toString(picked.getPayload()));
    }
  }

  private static class SimpleServiceDiscovered implements ServiceDiscovered {
    private final List<Discoverable> discoverables;

    SimpleServiceDiscovered(List<Discoverable> discoverables) {
      this.discoverables = discoverables;
    }

    @Override
    public String getName() {
      return null;
    }

    @Override
    public Cancellable watchChanges(ChangeListener changeListener, Executor executor) {
      return null;
    }

    @Override
    public boolean contains(Discoverable discoverable) {
      return false;
    }

    @Override
    public Iterator<Discoverable> iterator() {
      return discoverables.iterator();
    }
  }
}
