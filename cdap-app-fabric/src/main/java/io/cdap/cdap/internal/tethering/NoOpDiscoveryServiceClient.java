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

import io.cdap.cdap.master.spi.discovery.DefaultServiceDiscovered;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;

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
    Discoverable discoverable = new Discoverable(name, InetSocketAddress.createUnresolved(url.getHost(),
                                                                                          url.getPort()));
    serviceDiscovered.setDiscoverables(Collections.singleton(discoverable));
    return serviceDiscovered;
  }
}
