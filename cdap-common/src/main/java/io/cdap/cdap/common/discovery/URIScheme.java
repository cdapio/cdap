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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Enum representing URI scheme.
 */
public enum URIScheme {

  HTTP("http", new byte[0]),
  HTTPS("https", "https://".getBytes(StandardCharsets.UTF_8));

  private final String scheme;
  private final byte[] discoverablePayload;

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


  URIScheme(String scheme, byte[] discoverablePayload) {
    this.scheme = scheme;
    this.discoverablePayload = discoverablePayload;
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
}
