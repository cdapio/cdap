/*
 * Copyright © 2014 Cask Data, Inc.
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
package io.cdap.cdap.common.discovery;

import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;

import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * Randomly picks endpoint from the list of available endpoints.
 */
public final class RandomEndpointStrategy extends AbstractEndpointStrategy {

  /**
   * Constructs a random endpoint strategy with the given {@link ServiceDiscovered}.
   */
  public RandomEndpointStrategy(Supplier<ServiceDiscovered> serviceDiscoveredSupplier) {
    super(serviceDiscoveredSupplier);
  }

  @Override
  protected Discoverable pick(ServiceDiscovered serviceDiscovered) {
    return pickRandom(serviceDiscovered);
  }

  /**
   * Randomly picks a {@link Discoverable} from the given {@link ServiceDiscovered}.
   *
   * @param serviceDiscovered the {@link ServiceDiscovered} to pick from
   * @return a {@link Discoverable} or {@code null} if there is no discoverable available
   */
  @Nullable
  public static Discoverable pickRandom(ServiceDiscovered serviceDiscovered) {
    // Reservoir sampling
    Discoverable result = null;
    Iterator<Discoverable> itor = serviceDiscovered.iterator();
    System.out.println("wyzhang: start");
    int count = 0;
    while (itor.hasNext()) {
      System.out.println("wyzhang: start one iteration");
      Discoverable next = itor.next();
      if (ThreadLocalRandom.current().nextInt(++count) == 0) {
        result = next;
      }
    }
    System.out.println("wyzhang: end");
    return result;
  }
}
