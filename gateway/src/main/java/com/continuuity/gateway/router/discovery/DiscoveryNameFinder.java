package com.continuuity.gateway.router.discovery;

import com.continuuity.app.program.Type;

/**
 * Finds the discovery service name for a program.
 */
public interface DiscoveryNameFinder {

  /**
   * Find the discovery service name for a program.
   * @param type program type
   * @param name program name
   * @return discovery service name if available, else parameter name.
   */
  String findDiscoveryServiceName(Type type, String name);
}
