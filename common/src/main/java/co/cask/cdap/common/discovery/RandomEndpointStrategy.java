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
package co.cask.cdap.common.discovery;

import org.apache.twill.discovery.Discoverable;

import java.util.Iterator;
import java.util.Random;

/**
 * Randomly picks endpoint from the list of available endpoints.
 */
public final class RandomEndpointStrategy implements EndpointStrategy {

  private final Iterable<Discoverable> endpoints;

  /**
   * Constructs a random endpoint strategy.
   * @param endpoints Endpoints for the strategy to use. Note that this strategy will
   *                  invoke {@link Iterable#iterator()} and traverse through it on
   *                  every call to the {@link #pick()} method. One could leverage this
   *                  behavior with the live {@link Iterable} as provided by
   *                  {@link org.apache.twill.discovery.DiscoveryServiceClient#discover(String)} method.
   */
  public RandomEndpointStrategy(Iterable<Discoverable> endpoints) {
    this.endpoints = endpoints;
  }

  @Override
  public Discoverable pick() {
    // Reservoir sampling
    Discoverable result = null;
    Iterator<Discoverable> itor = endpoints.iterator();
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
