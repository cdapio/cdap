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
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.route.store.RouteConfig;
import co.cask.cdap.route.store.RouteStore;
import com.google.common.base.Strings;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;

/**
 * Picks an endpoint based on a distribution configuration from a list of available endpoints.
 */
public class DistributionEndpointStrategy extends AbstractEndpointStrategy {
  private final RouteStore routeStore;
  private final ProgramId serviceId;
  private final Random random;

  public DistributionEndpointStrategy(ServiceDiscovered serviceDiscovered, RouteStore routeStore, ProgramId serviceId) {
    super(serviceDiscovered);
    this.routeStore = routeStore;
    this.serviceId = serviceId;
    this.random = ThreadLocalRandom.current();
  }

  @Nullable
  @Override
  public Discoverable pick() {
    RouteConfig routeConfig = routeStore.fetch(serviceId);
    Iterator<Discoverable> iterator = serviceDiscovered.iterator();
    Discoverable result = null;
    double resultProbability = 0;
    while (iterator.hasNext()) {
      Discoverable candidate = iterator.next();
      String version = Bytes.toString(candidate.getPayload());
      double weight = 0;
      if (!Strings.isNullOrEmpty(version) && routeConfig != null) {
        Map<String, Integer> weights = routeConfig.getRoutes();
        Integer weightage = weights.get(version);
        if (weightage != null) {
          weight = weightage;
        }
      }
      double randomPick = random.nextDouble() * weight;
      // if pick probability is greater, retain the candidate
      if (randomPick >= resultProbability) {
        result = candidate;
        resultProbability = randomPick;
      }
    }
    return result;
  }
}
