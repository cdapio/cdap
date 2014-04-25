package com.continuuity.gateway.router.handlers;

import com.google.common.base.Objects;
import org.apache.twill.discovery.Discoverable;

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


  public String getHostName() {
    return discoverable.getSocketAddress().getHostName();
  }

  public int getPort() {
    return discoverable.getSocketAddress().getPort();
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
           Objects.equal(discoverable.getSocketAddress().getHostName(), that.getHostName()) &&
           Objects.equal(discoverable.getSocketAddress().getPort(), that.getPort());
  }
}
