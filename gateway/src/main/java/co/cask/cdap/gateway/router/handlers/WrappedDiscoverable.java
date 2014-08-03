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

package co.cask.cdap.gateway.router.handlers;

import com.google.common.base.Objects;
import org.apache.twill.discovery.Discoverable;

import java.net.InetSocketAddress;

/**
 * Wrapped discoverable is used to be used in other containers e.g., HashMap. Since the DiscoverableWrapper
 * is not available as a public class.
 */
public class WrappedDiscoverable {
  private final Discoverable discoverable;

  public WrappedDiscoverable(Discoverable discoverable) {
    this.discoverable = discoverable;
  }

  public String getName() {
    return discoverable.getName();
  }

  public InetSocketAddress getSocketAddress() {
    return discoverable.getSocketAddress();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(discoverable.getName(),
                            discoverable.getSocketAddress().getHostName(),
                            discoverable.getSocketAddress().getPort());
  }

  @Override
  public boolean equals(Object object) {
    if (object == null) {
      return false;
    }
    if (!(object instanceof WrappedDiscoverable)) {
      return false;
    }
    WrappedDiscoverable that = (WrappedDiscoverable) object;
    return Objects.equal(discoverable.getName(), that.getName()) &&
           Objects.equal(discoverable.getSocketAddress().getHostName(), that.getSocketAddress().getHostName()) &&
           Objects.equal(discoverable.getSocketAddress().getPort(), that.getSocketAddress().getPort());
  }
}
