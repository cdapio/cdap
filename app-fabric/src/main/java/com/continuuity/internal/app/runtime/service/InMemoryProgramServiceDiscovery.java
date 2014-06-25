package com.continuuity.internal.app.runtime.service;

import com.continuuity.internal.app.runtime.ProgramServiceDiscovery;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;

/**
 * InMemory ProgramServiceDiscovery which implements the discovery of User Service.
 */
public class InMemoryProgramServiceDiscovery implements ProgramServiceDiscovery {

  private DiscoveryServiceClient dsClient;

  @Inject
  public InMemoryProgramServiceDiscovery(DiscoveryServiceClient discoveryService) {
    this.dsClient = discoveryService;
  }

  @Override
  public ServiceDiscovered discover(String accountId, String appId, String serviceId, String serviceName) {
    return dsClient.discover(String.format("service.%s.%s.%s.%s", accountId, appId, serviceId, serviceName));
  }
}
