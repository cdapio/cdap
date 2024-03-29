/*
 * Copyright © 2015 Cask Data, Inc.
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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.twill.discovery.Discoverable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Discoverable that resolves 0.0.0.0 to a routable interface.
 */
public class ResolvingDiscoverable extends Discoverable {

  private static final Logger LOG = LoggerFactory.getLogger(ResolvingDiscoverable.class);

  private ResolvingDiscoverable(Discoverable discoverable) {
    super(discoverable.getName(), discoverable.getSocketAddress(), discoverable.getPayload());
  }

  public static ResolvingDiscoverable of(Discoverable discoverable) {
    return new ResolvingDiscoverable(discoverable);
  }

  @Override
  public InetSocketAddress getSocketAddress() {
    return resolve(super.getSocketAddress());
  }

  /**
   * Resolves the given {@link InetSocketAddress}.
   */
  public static InetSocketAddress resolve(InetSocketAddress bindAddress) {
    try {
      // If domain of bindAddress is not resolvable, address of bindAddress is null.
      if (bindAddress.getAddress() != null && bindAddress.getAddress().isAnyLocalAddress()) {
        return new InetSocketAddress(InetAddress.getLocalHost().getHostName(),
            bindAddress.getPort());
      }
    } catch (Exception e) {
      LOG.warn("Unable to resolve bindAddress", e);
    }
    return bindAddress;
  }
}
