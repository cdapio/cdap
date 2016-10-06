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
import co.cask.cdap.common.discovery.AbstractEndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.route.store.RouteConfig;
import co.cask.cdap.route.store.RouteStore;
import com.google.common.annotations.VisibleForTesting;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;

import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;

/**
 * Picks an endpoint based on a distribution configuration from a list of available endpoints.
 */
public class UserServiceEndpointStrategy extends AbstractEndpointStrategy {
  private final RouteStore routeStore;
  private final ProgramId serviceId;
  private final String version;
  private final Random random;
  private final RouteFallbackStrategy fallbackStrategy;
  private final RandomEndpointStrategy versionedRandomEndpointStrategy;

  public UserServiceEndpointStrategy(ServiceDiscovered serviceDiscovered, RouteStore routeStore, ProgramId serviceId,
                                     RouteFallbackStrategy fallbackStrategy, @Nullable String version) {
    super(serviceDiscovered);
    this.routeStore = routeStore;
    this.serviceId = serviceId;
    this.version = version;
    this.random = ThreadLocalRandom.current();
    this.fallbackStrategy = fallbackStrategy;
    this.versionedRandomEndpointStrategy = new RandomEndpointStrategy(
      new VersionFilteredServiceDiscovered(serviceDiscovered, version));
  }

  @VisibleForTesting
  UserServiceEndpointStrategy(ServiceDiscovered serviceDiscovered, RouteStore routeStore, ProgramId serviceId) {
    this(serviceDiscovered, routeStore, serviceId, RouteFallbackStrategy.RANDOM, null);
  }

  @Nullable
  @Override
  public Discoverable pick() {
    if (version != null) {
      return versionedRandomEndpointStrategy.pick();
    }

    RouteConfig routeConfig = routeStore.fetch(serviceId);
    if (!routeConfig.isValid()) {
      if (fallbackStrategy.equals(RouteFallbackStrategy.DROP)) {
        return null;
      }

      if (fallbackStrategy.equals(RouteFallbackStrategy.RANDOM)) {
        // Since the fallback strategy is RANDOM, we will do pull random pick across all discoverables
        return versionedRandomEndpointStrategy.pick();
      }
    }

    String minMaxVersion = null;
    Iterator<Discoverable> iterator = serviceDiscovered.iterator();
    Discoverable result = null;
    double resultProbability = 0;
    while (iterator.hasNext()) {
      Discoverable candidate = iterator.next();
      String version = Bytes.toString(candidate.getPayload());
      if (!routeConfig.isValid()) {
        // Fallback strategy is set to smallest/largest, so choose a random endpoint that has smallest/largest version
        if (minMaxVersion == null) {
          minMaxVersion = version;
        } else {
          int resetMinMaxVersion = minMaxVersion.compareTo(version);
          resetMinMaxVersion *= fallbackStrategy.equals(RouteFallbackStrategy.SMALLEST) ? 1 : -1;
          // If the comparison, shows a smaller/larger minMaxVersion, then reset the pick and
          // consider the current discoverable.
          // If the comparison, shows that the current discoverable, needs to be skipped, then continue
          // If the comparison, indicates the version to be same as the current smaller/larger version, then
          // consider it to be picked.
          if (resetMinMaxVersion > 0) {
            minMaxVersion = version;
            // Since we found a version smaller/larger than the previously seen minMaxVersion, then reset the pick
            result = null;
            resultProbability = 0;
          } else if (resetMinMaxVersion < 0) {
            continue;
          }
        }
      }

      double randomPick = random.nextDouble() * fetchWeight(version, routeConfig);
      // if pick probability is greater, retain the candidate
      if (randomPick >= resultProbability) {
        result = candidate;
        resultProbability = randomPick;
      }
    }
    return result;
  }

  private double fetchWeight(@Nullable String version, RouteConfig routeConfig) {
    if (!routeConfig.isValid() || (version == null)) {
      // If route is not present, then allow some randomness by setting non-zero weight
      return 1;
    }

    Integer weight = routeConfig.getRoutes().get(version);
    if (weight == null) {
      // If this version is not present in the routeConfig, then don't choose that discoverable
      return 0;
    }
    return weight;
  }
}
