package com.continuuity.discovery;

import com.continuuity.base.Cancellable;
import com.google.common.util.concurrent.Service;

/**
 *
 */
public interface DiscoveryService extends Service {

  /**
   * Registers a {@link Discoverable} service.
   * @param discoverable Information of the service provider that could be discovered.
   * @return A {@link Cancellable} for un-registration.
   */
  Cancellable register(Discoverable discoverable);
}
