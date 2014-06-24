package com.continuuity.internal.app.runtime.service;

import com.continuuity.internal.app.runtime.ProgramServiceDiscovery;
import com.google.inject.Inject;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.discovery.ServiceDiscovered;

/**
 * InMemory ProgramServiceDiscovery which implements the discovery of User Service.
 */
public class InMemoryProgramServiceDiscovery implements ProgramServiceDiscovery {

  private InMemoryDiscoveryService discoveryService;

  @Inject
  public InMemoryProgramServiceDiscovery(InMemoryDiscoveryService discoveryService) {
    this.discoveryService = discoveryService;
  }

  @Override
  public ServiceDiscovered discover(String accId, String appId, String serviceId, String serviceName) {
    return discoveryService.discover(serviceName);
  }
}
