/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package io.cdap.cdap.explore.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.explore.service.Explore;
import io.cdap.cdap.security.URIScheme;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.common.http.HttpRequestConfig;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.cdap.cdap.common.conf.Constants.Service;

/**
 * An Explore Client that talks to a server implementing {@link Explore} over HTTP,
 * and that uses discovery to find the endpoints.
 */
public class DiscoveryExploreClient extends AbstractExploreClient {
  private final EndpointStrategy endpointStrategy;
  private final HttpRequestConfig httpRequestConfig;
  private final AuthenticationContext authenticationContext;

  @Inject
  @VisibleForTesting
  public DiscoveryExploreClient(final DiscoveryServiceClient discoveryClient,
                                AuthenticationContext authenticationContext) {
    this.endpointStrategy = new RandomEndpointStrategy(
      () -> discoveryClient.discover(Service.EXPLORE_HTTP_USER_SERVICE));
    this.httpRequestConfig = new DefaultHttpRequestConfig(false);
    this.authenticationContext = authenticationContext;
  }

  @Override
  protected HttpRequestConfig getHttpRequestConfig() {
    return httpRequestConfig;
  }

  @Override
  protected InetSocketAddress getExploreServiceAddress() {
    return getDiscoverable().getSocketAddress();
  }

  // This class is only used internally.
  // It does not go through router, so it doesn't ever need an auth token, sslEnabled, or verifySSLCert.

  @Override
  protected String getAuthToken() {
    return null;
  }

  @Override
  protected boolean isSSLEnabled() {
    return URIScheme.HTTPS.isMatch(getDiscoverable());
  }

  @Override
  protected boolean verifySSLCert() {
    return false;
  }

  // when run from programs, the user id will be set in the authentication context
  @Override
  protected String getUserId() {
    return authenticationContext.getPrincipal().getName();
  }

  // when run from programs, the user principal will be set in the authentication context
  @Override
  protected Map<String, String> addAdditionalSecurityHeaders() {
    return Collections.singletonMap(Constants.Security.Headers.USER_PRINCIPAL,
                                    authenticationContext.getPrincipal().getKerberosPrincipal());
  }

  private Discoverable getDiscoverable() {
    Discoverable discoverable = endpointStrategy.pick(3L, TimeUnit.SECONDS);
    if (discoverable == null) {
      throw new ServiceUnavailableException(Service.EXPLORE_HTTP_USER_SERVICE);
    }
    return discoverable;
  }
}
