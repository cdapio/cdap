/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.tethering;

import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.master.spi.discovery.DefaultServiceDiscovered;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;

/**
 * A DiscoveryServiceClient implementation that will always return the same url.
 */
public class NoOpDiscoveryServiceClient implements DiscoveryServiceClient {

  private URL url;

  public NoOpDiscoveryServiceClient(String url) throws MalformedURLException {
    this.url = new URL(url);
  }

  @Override
  public ServiceDiscovered discover(String name) {
    DefaultServiceDiscovered serviceDiscovered = new DefaultServiceDiscovered(name);
    URIScheme uriScheme;
    if (url.getProtocol().equals(URIScheme.HTTPS.getScheme())) {
      uriScheme = URIScheme.HTTPS;
    } else if (url.getProtocol().equals(URIScheme.HTTP.getScheme())) {
      uriScheme = URIScheme.HTTP;
    } else {
      throw new IllegalArgumentException(String.format("Invalid protocol:%s", url.getProtocol()));
    }
    int port = url.getPort() != -1 ? url.getPort() : uriScheme.getDefaultPort();
    Discoverable discoverable = uriScheme.createDiscoverable(name,
        InetSocketAddress.createUnresolved(url.getHost(), port));
    serviceDiscovered.setDiscoverables(Collections.singleton(discoverable));
    return serviceDiscovered;
  }
}
