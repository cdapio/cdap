/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.common.discovery;

import io.cdap.http.NettyHttpService;
import org.apache.twill.discovery.Discoverable;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Enum representing URI scheme.
 */
public enum URIScheme {

  HTTP("http", new byte[0], 80),
  HTTPS("https", "https://".getBytes(StandardCharsets.UTF_8), 443);

  private final String scheme;
  private final byte[] discoverablePayload;
  private final int defaultPort;

  /**
   * Returns the {@link URIScheme} based on the given {@link Discoverable} payload.
   */
  public static URIScheme getScheme(Discoverable discoverable) {
    for (URIScheme scheme : values()) {
      if (scheme.isMatch(discoverable)) {
        return scheme;
      }
    }
    // Default to HTTP
    return HTTP;
  }

  /**
   * Creates a {@link Discoverable} from the given service name and URL.
   */
  public static Discoverable createDiscoverable(String serviceName, URL url) {
    String protocol = url.getProtocol();
    for (URIScheme scheme : values()) {
      if (scheme.scheme.equalsIgnoreCase(protocol)) {
        int port = url.getPort();
        return scheme.createDiscoverable(serviceName,
                                         new InetSocketAddress(url.getHost(), port == -1 ? scheme.defaultPort : port));
      }
    }
    throw new IllegalArgumentException("Unsupported protocol from URL " + url);
  }

  /**
   * Creates a {@link Discoverable} for the given service name that represents the given {@link NettyHttpService}.
   */
  public static Discoverable createDiscoverable(String serviceName, NettyHttpService httpService) {
    URIScheme scheme = httpService.isSSLEnabled() ? HTTPS : HTTP;
    return scheme.createDiscoverable(serviceName, httpService.getBindAddress());
  }

  /**
   * Creates a {@link URI} based on the scheme from the given {@link Discoverable}.
   */
  public static URI createURI(Discoverable discoverable, String pathFmt, Object...objs) {
    String scheme = getScheme(discoverable).scheme;
    InetSocketAddress address = discoverable.getSocketAddress();
    String path = String.format(pathFmt, objs);
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    return URI.create(String.format("%s://%s:%d/%s", scheme, address.getHostName(), address.getPort(), path));
  }


  URIScheme(String scheme, byte[] discoverablePayload, int defaultPort) {
    this.scheme = scheme;
    this.discoverablePayload = discoverablePayload;
    this.defaultPort = defaultPort;
  }

  /**
   * Returns {@code true} if the given {@link Discoverable} has payload that matches with this scheme.
   */
  public boolean isMatch(Discoverable discoverable) {
    return Arrays.equals(discoverablePayload, discoverable.getPayload());
  }

  /**
   * Creates a {@link Discoverable} with the payload representing this scheme.
   */
  public Discoverable createDiscoverable(String name, InetSocketAddress address) {
    return new Discoverable(name, address, discoverablePayload);
  }

  /**
   * Returns the scheme.
   */
  public String getScheme() {
    return scheme;
  }
}
