package com.continuuity.common.discovery;

import com.google.common.collect.ImmutableMap;
import com.netflix.curator.x.discovery.ServiceInstance;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.Collection;
import java.util.Map;

/**
 *
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface ServiceDiscoveryClient {
  void register(String name, int port, Map<String, String> payload) throws ServiceDiscoveryClientException;
  void unregister(String name) throws ServiceDiscoveryClientException;
  int  getProviderCount(String name) throws ServiceDiscoveryClientException;
}
