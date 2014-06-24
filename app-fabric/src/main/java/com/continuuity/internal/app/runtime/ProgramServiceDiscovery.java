package com.continuuity.internal.app.runtime;

import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.ServiceDiscovered;

/**
 * User Service Discovery using Runtime Context.
 */
public interface ProgramServiceDiscovery {
  ServiceDiscovered discover(String accId, String appId, String serviceId, String serviceName);
}
