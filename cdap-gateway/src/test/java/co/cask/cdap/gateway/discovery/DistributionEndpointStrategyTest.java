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
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.service.ServiceDiscoverable;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.route.store.RouteConfig;
import co.cask.cdap.route.store.RouteStore;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;

/**
 * Tests for {@link DistributionEndpointStrategy}
 */
public class DistributionEndpointStrategyTest {
  private static final Logger LOG = LoggerFactory.getLogger(DistributionEndpointStrategyTest.class);

  @Test
  public void testOneVersion() throws Exception {
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
    DistributionEndpointStrategy strategy = new DistributionEndpointStrategy(serviceDiscovered, configStore, serviceId);

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

  private static class InMemoryRouteStore implements RouteStore {
    private final Map<ProgramId, RouteConfig> routeMap;

    public InMemoryRouteStore(Map<ProgramId, RouteConfig> routeMap) {
      this.routeMap = Maps.newHashMap(routeMap);
    }

    @Override
    public void store(ProgramId serviceId, RouteConfig routeConfig) {
      routeMap.put(serviceId, routeConfig);
    }

    @Override
    public void delete(ProgramId serviceId) throws NotFoundException {
      routeMap.remove(serviceId);
    }

    @Nullable
    @Override
    public RouteConfig fetch(ProgramId serviceId) {
      return routeMap.get(serviceId);
    }

    @Override
    public void close() throws Exception {
      // nothing to do
    }
  }
}
