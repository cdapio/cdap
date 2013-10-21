package com.continuuity.internal.app.runtime.webapp;

import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

/**
 * Determines the path to serve based on the Host header.
 */
public class ServePathGenerator {
  public static final String SRC_PATH = "/src/";
  public static final String DEFAULT_DIR_NAME = "default";

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

    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
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
      servePath = findPath(hostHeader.substring(0, hostHeader.length() - DEFAULT_PORT_STR.length()), path);
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

    // Else if "default" is present, that is the serve dir
    servePath = findPath(DEFAULT_DIR_NAME, path);
    if (servePath != null) {
      return servePath;
    }

    return null;
  }

  private String findPath(String hostHeader, String path) {
    // First try firstPathPart/src/restPath
    Iterable<String> pathParts = Splitter.on('/').limit(2).split(path);
    String servePath;
    if (Iterables.size(pathParts) > 1) {
      servePath = String.format("%s/%s/%s%s%s", baseDir, hostHeader,
                                       Iterables.get(pathParts, 0), SRC_PATH, Iterables.get(pathParts, 1));
      if (fileExists.apply(servePath)) {
        return servePath;
      }

    } else if (Iterables.size(pathParts) == 1) {
      servePath = String.format("%s/%s/%s%s%s", baseDir, hostHeader,
                                Iterables.get(pathParts, 0), SRC_PATH, "index.html");
      if (fileExists.apply(servePath)) {
        return servePath;
      }
    }

    // Next try src/path
    path = path.isEmpty() ? "index.html" : path;
    servePath = String.format("%s/%s%s%s", baseDir, hostHeader, SRC_PATH, path);
    if (fileExists.apply(servePath)) {
      return servePath;
    }

    return null;
  }
}
