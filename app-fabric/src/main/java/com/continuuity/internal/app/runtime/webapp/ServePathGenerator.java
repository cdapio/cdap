package com.continuuity.internal.app.runtime.webapp;

import com.google.common.base.Predicate;

/**
 * Determines the path to serve based on the Host header.
 */
public class ServePathGenerator {
  private static final String DEFAULT_DIR_NAME = "default";
  private static final String DEFAULT_PORT_STR = ":80";

  private final String baseDir;
  private final Predicate<String> fileExists;

  public ServePathGenerator(String baseDir, Predicate<String> fileExists) {
    this.baseDir = baseDir.replaceAll("/+$", "");
    this.fileExists = fileExists;
  }

  public String getServePath(String hostHeader, String path) {
    if (path.startsWith("/")) {
      path = path.substring(1);
    }

    // If exact match present, return it
    String servePath = findPath(hostHeader, path);
    if (servePath != null) {
      return servePath;
    }

    boolean isDefaultPort = hostHeader.endsWith(DEFAULT_PORT_STR);
    boolean hasNoPort = hostHeader.indexOf(':') == -1;

    // Strip DEFAULT_PORT_STR and try again
    if (isDefaultPort) {
      servePath = findPath(hostHeader.substring(0, hostHeader.length() - 3), path);
      if (servePath != null) {
        return servePath;
      }
    }

    // Add DEFAULT_PORT_STR and try
    if (hasNoPort) {
      servePath = findPath(hostHeader + DEFAULT_PORT_STR, path);
      if (servePath != null) {
        return servePath;
      }
    }

    // Else see if "default":port dir is present
    int portInd = hostHeader.lastIndexOf(':');
    String port = portInd == -1 ? "" : hostHeader.substring(portInd);
    servePath = findPath(DEFAULT_DIR_NAME + port, path);
    if (servePath != null) {
      return servePath;
    }

    // Else if "default" is present, that is the serve dir
    servePath = findPath(DEFAULT_DIR_NAME, path);
    if (servePath != null) {
      return servePath;
    }

    return null;
  }

  private String findPath(String hostHeader, String path) {
    String servePath = String.format("%s/%s/%s", baseDir, hostHeader, path);
    if (fileExists.apply(servePath)) {
      return servePath;
    }

    return null;
  }
}
