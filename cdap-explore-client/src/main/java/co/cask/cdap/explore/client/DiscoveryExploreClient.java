/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.explore.client;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.http.DefaultHttpRequestConfig;
import co.cask.cdap.explore.service.Explore;
import co.cask.common.http.HttpRequestConfig;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static co.cask.cdap.common.conf.Constants.Service;

/**
 * An Explore Client that talks to a server implementing {@link Explore} over HTTP,
 * and that uses discovery to find the endpoints.
 */
public class DiscoveryExploreClient extends AbstractExploreClient {
  private final Supplier<EndpointStrategy> endpointStrategySupplier;
  private final HttpRequestConfig httpRequestConfig;

  @Inject
  public DiscoveryExploreClient(CConfiguration cConf, final DiscoveryServiceClient discoveryClient) {
    this.endpointStrategySupplier = Suppliers.memoize(new Supplier<EndpointStrategy>() {
      @Override
      public EndpointStrategy get() {
        return new RandomEndpointStrategy(discoveryClient.discover(Service.EXPLORE_HTTP_USER_SERVICE));
      }
    });

    this.httpRequestConfig = new DefaultHttpRequestConfig();
  }

  @Override
  protected HttpRequestConfig getHttpRequestConfig() {
    return httpRequestConfig;
  }

  @Override
  protected InetSocketAddress getExploreServiceAddress() {
    Discoverable discoverable = endpointStrategySupplier.get().pick(3L, TimeUnit.SECONDS);
    if (discoverable != null) {
      return discoverable.getSocketAddress();
    }
    throw new RuntimeException(
      String.format("Cannot discover service %s", Service.EXPLORE_HTTP_USER_SERVICE));
  }

  // This class is only used internally.
  // It does not go through router, so it doesn't ever need an auth token, sslEnabled, or verifySSLCert.

  @Override
  protected String getAuthToken() {
    return null;
  }


  @Override
  protected boolean isSSLEnabled() {
    return false;
  }

  @Override
  protected boolean verifySSLCert() {
    return false;
  }
}
