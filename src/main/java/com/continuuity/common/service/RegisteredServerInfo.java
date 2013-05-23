package com.continuuity.common.service;

import com.continuuity.common.discovery.ServicePayload;

/**
 * Class representing the server info for a registered server.
 */
public class RegisteredServerInfo {
  private ServicePayload payload = new ServicePayload();
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

  public ServicePayload getPayload() {
    return payload;
  }

}
