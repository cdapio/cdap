/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.kafka.client;

/**
 * Represents broker information. This class is instantiated by gson.
 */
public final class BrokerInfo {

  private String host;
  private int port;

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }
}
