/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.kafka.client;

import com.google.common.base.Objects;

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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BrokerInfo that = (BrokerInfo) o;
    return host.equals(that.getHost()) && port == that.getPort();
  }

  @Override
  public int hashCode() {
    int result = host.hashCode();
    result = 31 * result + port;
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(BrokerInfo.class)
                  .add("host", host)
                  .add("port", port)
                  .toString();
  }
}
