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
import co.cask.cdap.common.discovery.AbstractEndpointStrategy;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.route.store.RouteConfig;
import co.cask.cdap.route.store.RouteStore;
import com.google.common.base.Strings;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.annotation.Nullable;

/**
 * Picks an endpoint based on a distribution configuration from a list of available endpoints.
 */
public class DistributionEndpointStrategy extends AbstractEndpointStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(DistributionEndpointStrategy.class);
  private final RouteStore routeStore;
  private final ProgramId serviceId;

  public DistributionEndpointStrategy(ServiceDiscovered serviceDiscovered, RouteStore routeStore, ProgramId serviceId) {
    super(serviceDiscovered);
    this.routeStore = routeStore;
    this.serviceId = serviceId;
  }

  @Nullable
  @Override
  public Discoverable pick() {
    DiscoverableBucket discoverableBucket = new DiscoverableBucket();
    Iterator<Discoverable> iterator = serviceDiscovered.iterator();
    while (iterator.hasNext()) {
      discoverableBucket.put(iterator.next());
    }
    RouteConfig routeConfig = null;
    try {
      routeConfig = routeStore.fetch(serviceId);
    } catch (NotFoundException ex) {
      LOG.debug("Could not find route config for service {}", serviceId, ex);
    }
    return discoverableBucket.getDiscoverable(routeConfig);
  }

  private static class DiscoverableBucket {
    private final Map<String, List<Discoverable>> buckets;
    private final Random random;
    private final List<Discoverable> defaultList;

    DiscoverableBucket() {
      this.random = new Random();
      this.buckets = new HashMap<>();
      // default bucket
      this.defaultList = new ArrayList<>();
      buckets.put("", defaultList);
    }

    void put(Discoverable discoverable) {
      String versionId = Bytes.toString(discoverable.getPayload());
      defaultList.add(discoverable);
      if (!Strings.isNullOrEmpty(versionId)) {
        List<Discoverable> discoverableList;
        if (!buckets.containsKey(versionId)) {
          discoverableList = buckets.put(versionId, new ArrayList<Discoverable>());
        } else {
          discoverableList = buckets.get(versionId);
        }
        discoverableList.add(discoverable);
      }
    }

    Discoverable getDiscoverable(RouteConfig routeConfig) {
      String version = "";
      if (routeConfig != null) {
        int randomInt = random.nextInt(100);
        int sumSoFar = 0;
        for (Map.Entry<String, Integer> versionKey : routeConfig.getRoutes().entrySet()) {
          int newSum = sumSoFar + versionKey.getValue();
          // Does it fall in that bucket
          if (randomInt >= sumSoFar && randomInt < newSum) {
            version = versionKey.getKey();
            break;
          }
        }
      }

      if (!buckets.containsKey(version)) {
        version = "";
      }
      int randomIndex = random.nextInt(buckets.get(version).size());
      return buckets.get(version).get(randomIndex);
    }
  }
}
