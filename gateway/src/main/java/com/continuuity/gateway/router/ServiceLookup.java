package com.continuuity.gateway.router;

import java.util.Map;

/**
 * Interface to lookup services that the router forwards.
 */
public interface ServiceLookup {
  /**
   * Lookup service name given port.
   *
   * @param port port to lookup.
   * @return service name.
   */
  String getService(int port);

  /**
   * @return the port to service name map for all services.
   */
  Map<Integer, String> getServiceMap();
}
