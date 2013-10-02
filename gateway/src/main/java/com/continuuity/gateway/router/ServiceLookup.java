package com.continuuity.gateway.router;

import com.continuuity.weave.discovery.Discoverable;
import com.google.common.base.Supplier;

import java.util.Map;

/**
 * Interface to lookup services that the router forwards.
 */
public interface ServiceLookup {
  /**
   * Lookup service name given port.
   *
   * @param port port to lookup.
   * @return service name based on port.
   */
  String getService(int port);

  /**
   * Returns the discoverable mapped to the given port.
   *
   * @param port port to lookup.
   * @param hostHeaderSupplier supplies the Host header for the lookup.
   * @return discoverable based on port and host header.
   */
  Discoverable getDiscoverable(int port, Supplier<String> hostHeaderSupplier) throws Exception;

  /**
   * @return the port to service name map for all services.
   */
  Map<Integer, String> getServiceMap();
}
