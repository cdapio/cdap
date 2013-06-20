/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Utility class to provide methods for common network related operations.
 */
public final class Networks {

  /**
   * Resolves the given hostname into {@link InetAddress}.
   *
   * @param hostname The hostname in String. If {@code null}, return localhost.
   * @param onErrorAddress InetAddress to return if the given hostname cannot be resolved.
   * @return An {@link InetAddress} of the resolved hostname.
   */
  public static InetAddress resolve(String hostname, InetAddress onErrorAddress) {
    try {
      if (hostname != null) {
        return InetAddress.getByName(hostname);
      } else {
        return InetAddress.getLocalHost();
      }
    } catch (UnknownHostException e) {
      return onErrorAddress;
    }
  }

  private Networks() {
  }
}
