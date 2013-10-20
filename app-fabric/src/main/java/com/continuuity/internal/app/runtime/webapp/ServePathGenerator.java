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
    this.baseDir = baseDir;
    this.fileExists = fileExists;
  }

  public String getServePath(String hostHeader, String path) {
    // If exact match present, return it
    String servePath = String.format("%s/%s/%s", baseDir, hostHeader, path);
    if (fileExists.apply(servePath)) {
      return servePath;
    }

    boolean isDefaultPort = hostHeader.endsWith(DEFAULT_PORT_STR);
    boolean hasNoPort = hostHeader.indexOf(':') == -1;

    // Strip DEFAULT_PORT_STR and try again
    if (isDefaultPort) {
      servePath = String.format("%s/%s/%s", baseDir,
                                hostHeader.substring(0, hostHeader.length() - 3), path);
      if (fileExists.apply(servePath)) {
        return servePath;
      }
    }

    // Add DEFAULT_PORT_STR and try
    if (hasNoPort) {
      servePath = String.format("%s/%s%s/%s", baseDir,
                                hostHeader, DEFAULT_PORT_STR, path);
      if (fileExists.apply(servePath)) {
        return servePath;
      }
    }

    // Else see if "default":port dir is present
    String port = hostHeader.substring(hostHeader.lastIndexOf(':') + 1);
    servePath = String.format("%s/%s:%s/%s", baseDir, DEFAULT_DIR_NAME, port, path);
    if (fileExists.apply(servePath)) {
      return servePath;
    }

    // Else if "default" is present, that is the serve dir
    if (isDefaultPort || hasNoPort) {
      servePath = String.format("%s/%s/%s", baseDir, DEFAULT_DIR_NAME, path);
      if (fileExists.apply(servePath)) {
        return servePath;
      }
    }

    return null;
  }
}
