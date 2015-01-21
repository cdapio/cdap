/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.common.discovery;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A helper class to help using {@link DiscoveryServiceClient} and {@link ServiceDiscovered} correctly.
 */
public final class ServiceDiscoveries {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceDiscoveries.class);

  /**
   * Creates an URL based on the endpoint discovered by the given service name. If there are multiple endpoints
   * available, one will be picked randomly.
   *
   * @param discoveryClient the discovery client to discover endpoints
   * @param serviceName name of the service for endpoint discovery
   * @param protocol protocol for the resulting {@link URL}
   * @param path path for the resulting {@link URL}
   * @param timeout maximum time to spend to wait for endpoint to be available
   * @param unit unit for the timeout
   * @return an URL in the format of {@code protocol://host:port/path} or {@code null} if no endpoint is available
   */
  @Nullable
  public static URL discoverURL(DiscoveryServiceClient discoveryClient,
                                String serviceName, final String protocol, final String path,
                                long timeout, TimeUnit unit) {

    ServiceDiscovered serviceDiscovered = discoveryClient.discover(serviceName);
    URL url = createURL(new RandomEndpointStrategy(serviceDiscovered), protocol, path);
    if (url != null) {
      return url;
    }

    final SettableFuture<URL> result = SettableFuture.create();
    final Cancellable discoveryCancel = serviceDiscovered.watchChanges(new ServiceDiscovered.ChangeListener() {
      @Override
      public void onChange(ServiceDiscovered serviceDiscovered) {
        URL url = createURL(new RandomEndpointStrategy(serviceDiscovered), protocol, path);
        if (url != null) {
          result.set(url);
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    try {
      return result.get(timeout, unit);
    } catch (Exception e) {
      LOG.debug("No discovery available for service {}", serviceName);
    } finally {
      discoveryCancel.cancel();
    }
    return null;
  }

  @Nullable
  private static URL createURL(EndpointStrategy endpointStrategy, String protocol, String path) {
    Discoverable discoverable = endpointStrategy.pick();
    if (discoverable == null) {
      return null;
    }
    try {
      if (path.charAt(0) != '/') {
        path = '/' + path;
      }
      InetSocketAddress address = discoverable.getSocketAddress();
      return new URL(protocol, address.getHostName(), address.getPort(), path);
    } catch (MalformedURLException e) {
      LOG.warn("Malformed URL", e);
      return null;
    }
  }

  private ServiceDiscoveries() {
  }
}
