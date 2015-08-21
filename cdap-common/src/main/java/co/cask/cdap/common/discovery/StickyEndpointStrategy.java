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

/**
 * An {@link EndpointStrategy} that will always return the same endpoint once it's picked
 * until the endpoint is no longer valid, then it'll pick another one.
 *
 * If multiple threads calling the {@link #pick()}} method at the same time, it's possible
 * that they get different result if there was no endpoint being picked yet or the previously
 * picked endpoint is no longer value. The pick will be eventually settled to the same one.
 */
public final class StickyEndpointStrategy extends AbstractEndpointStrategy {

  private final EndpointStrategy picker;
  private volatile Discoverable lastPick;

  public StickyEndpointStrategy(ServiceDiscovered serviceDiscovered) {
    super(serviceDiscovered);
    this.picker = new RandomEndpointStrategy(serviceDiscovered);
  }

  @Override
  public Discoverable pick() {
    Discoverable lastPick = this.lastPick;
    if (lastPick == null || !isValid(lastPick)) {
      this.lastPick = lastPick = picker.pick();
    }
    return lastPick;
  }

  private boolean isValid(Discoverable endpoint) {
    for (Discoverable discoverable : serviceDiscovered) {
      if (discoverable.getSocketAddress().equals(endpoint.getSocketAddress())) {
        return true;
      }
    }
    return false;
  }
}
