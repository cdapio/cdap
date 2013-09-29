package com.continuuity.gateway.router;

import com.continuuity.common.discovery.EndpointStrategy;

/**
 * Information about a service and its discoverable endpoint strategy.
 */
public class DiscoverableService {
  private final String serviceName;
  private final EndpointStrategy endpointStrategy;

  public DiscoverableService(String serviceName, EndpointStrategy endpointStrategy) {
    this.serviceName = serviceName;
    this.endpointStrategy = endpointStrategy;
  }

  public String getServiceName() {
    return serviceName;
  }

  public EndpointStrategy getEndpointStrategy() {
    return endpointStrategy;
  }
}
