package com.continuuity.discovery;

import com.google.common.util.concurrent.Service;

/**
 *
 */
public interface DiscoveryServiceClient extends Service {

  /**
   * Retrieves a list of {@link Discoverable} for the a service with the given name.
   *
   * @param name Name of the service
   * @return A live {@link Iterable} that on each call to {@link Iterable#iterator()} returns
   *         an {@link java.util.Iterator Iterator} that reflects the latest set of
   *         available {@link Discoverable} services.
   */
  Iterable<Discoverable> discover(String name);
}
