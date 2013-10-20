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
   * Removes ":80" from end of the host, replaces '.', ':', '/' and '-' with '_' and URL encodes it.
   * @param host host that needs to be normalized.
   * @return the normalized host.
   */
  public static String normalizeWebappHost(String host) throws UnsupportedEncodingException {
    if (host.endsWith(":80")) {
      host = host.substring(0, host.length() - 3);
    }

    host = host.replace('.', '_');
    host = host.replace('-', '_');
    host = host.replace('/', '_');
    host = host.replace(':', '_');

    return URLEncoder.encode(host, Charsets.UTF_8.name());
  }
}
