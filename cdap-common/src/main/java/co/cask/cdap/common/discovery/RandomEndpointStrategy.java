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
package co.cask.cdap.common.discovery;

import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;

import java.util.Iterator;
import java.util.Random;

/**
 * Randomly picks endpoint from the list of available endpoints.
 */
public final class RandomEndpointStrategy extends AbstractEndpointStrategy {

  /**
   * Constructs a random endpoint strategy with the given {@link ServiceDiscovered}.
   */
  public RandomEndpointStrategy(ServiceDiscovered serviceDiscovered) {
    super(serviceDiscovered);
  }

  @Override
  public Discoverable pick() {
    // Reservoir sampling
    Discoverable result = null;
    Iterator<Discoverable> itor = serviceDiscovered.iterator();
    Random random = new Random();
    int count = 0;
    while (itor.hasNext()) {
      Discoverable next = itor.next();
      if (random.nextInt(++count) == 0) {
        result = next;
      }
    }
    return result;
  }
}
