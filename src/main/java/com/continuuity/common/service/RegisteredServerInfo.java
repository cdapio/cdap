package com.continuuity.common.service;

import com.continuuity.common.discovery.ServiceDiscoveryClient;

/**
 * Class representing the server info for a registered server.
 */
public class RegisteredServerInfo {
  private ServiceDiscoveryClient.ServicePayload payload = new ServiceDiscoveryClient.ServicePayload();
  private int port;
  private String address;

  public RegisteredServerInfo(String address, int port) {
    this.address = address;
    this.port = port;
  }

  public int getPort() {
    return port;
  }

  public String getAddress() {
    return address;
  }

  public void addPayload(String key, String value) {
    payload.add(key, value);
  }

  public ServiceDiscoveryClient.ServicePayload getPayload() {
    return payload;
  }

}