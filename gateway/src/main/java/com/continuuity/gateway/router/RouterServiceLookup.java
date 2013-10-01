package com.continuuity.gateway.router;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Port -> service lookup.
 */
public class RouterServiceLookup implements ServiceLookup {
  private final AtomicReference<Map<Integer, String>> serviceMapRef =
    new AtomicReference<Map<Integer, String>>(ImmutableMap.<Integer, String>of());

  @Override
  public String getService(int port) {
    return serviceMapRef.get().get(port);
  }

  @Override
  public Map<Integer, String> getServiceMap() {
    return ImmutableMap.copyOf(serviceMapRef.get());
  }

  public void updateServiceMap(Map<Integer, String> serviceMap) {
    serviceMapRef.set(serviceMap);
  }
}
