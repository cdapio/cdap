/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.utils;

import com.google.common.base.Charsets;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URLEncoder;
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

  /**
   * Find a random free port in localhost for binding.
   * @return A port number or -1 for failure.
   */
  public static int getRandomPort() {
    try {
      ServerSocket socket = new ServerSocket(0);
      try {
        return socket.getLocalPort();
      } finally {
        socket.close();
      }
    } catch (IOException e) {
      return -1;
    }
  }

  private Networks() {
  }

  /**
   * Normalizes the name by doing the following:
   * <ul>
   *   <li>Remove trailing slashes.</li>
   *   <li>Remove :80 from end of the host part if any.</li>
   *   <li>Replace '.', ':', '/' and '-' with '_'.</li>
   *   <li>URL encode the name.</li>
   * </ul>
   * @param name discovery name that needs to be normalized.
   * @return the normalized discovery name.
   */
  public static String normalizeWebappDiscoveryName(String name) throws UnsupportedEncodingException {
    if (name.endsWith("/")) {
      name = name.replaceAll("/+$", "");
    }

    if (name.contains(":80/")) {
      name = name.replace(":80/", "/");
    } else if (name.endsWith(":80")) {
      name = name.substring(0, name.length() - 3);
    }

    name = name.replace('.', '_');
    name = name.replace('-', '_');
    name = name.replace(':', '_');
    name = name.replace('/', '_');

    return URLEncoder.encode(name, Charsets.UTF_8.name());
  }
}
