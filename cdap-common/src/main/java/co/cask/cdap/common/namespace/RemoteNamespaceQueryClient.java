/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.common.namespace;

import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.http.DefaultHttpRequestConfig;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import co.cask.http.HttpHandler;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link NamespaceQueryAdmin} that pings the internal endpoints in a {@link HttpHandler} in remote
 * system service.
 */
public class RemoteNamespaceQueryClient extends AbstractNamespaceQueryClient {
  private final Supplier<EndpointStrategy> endpointStrategySupplier;

  @Inject
  RemoteNamespaceQueryClient(final DiscoveryServiceClient discoveryClient) {
    this.endpointStrategySupplier = Suppliers.memoize(new Supplier<EndpointStrategy>() {
      @Override
      public EndpointStrategy get() {
        return new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.REMOTE_SYSTEM_OPERATION));
      }
    });
  }

  @Override
  protected HttpResponse execute(HttpRequest request) throws IOException {
    return HttpRequests.execute(request, new DefaultHttpRequestConfig());
  }

  @Override
  protected URL resolve(String resource) throws IOException {
    InetSocketAddress addr = getNamespaceServiceAddress();
    String url = String.format("http://%s:%d/v1/execute/%s", addr.getHostName(), addr.getPort(), resource);
    return new URL(url);
  }

  private InetSocketAddress getNamespaceServiceAddress() {
    Discoverable discoverable = endpointStrategySupplier.get().pick(3L, TimeUnit.SECONDS);
    if (discoverable != null) {
      return discoverable.getSocketAddress();
    }
    throw new ServiceUnavailableException(Constants.Service.REMOTE_SYSTEM_OPERATION);
  }
}
