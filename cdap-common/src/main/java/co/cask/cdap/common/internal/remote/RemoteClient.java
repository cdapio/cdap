/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.common.internal.remote;

import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequestConfig;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Discovers a remote service and resolves URLs to that service.
 */
public class RemoteClient {
  private final Supplier<EndpointStrategy> endpointStrategySupplier;
  private final HttpRequestConfig httpRequestConfig;
  private final String discoverableServiceName;
  private final String basePath;

  public RemoteClient(final DiscoveryServiceClient discoveryClient, final String discoverableServiceName,
                      HttpRequestConfig httpRequestConfig, String basePath) {
    this.discoverableServiceName = discoverableServiceName;
    this.httpRequestConfig = httpRequestConfig;
    // Use a supplier to delay the discovery until the first time it is being used.
    this.endpointStrategySupplier = Suppliers.memoize(new Supplier<EndpointStrategy>() {
      @Override
      public EndpointStrategy get() {
        return new RandomEndpointStrategy(discoveryClient.discover(discoverableServiceName));
      }
    });
    String cleanBasePath = basePath.startsWith("/") ? basePath : "/" + basePath;
    this.basePath = cleanBasePath.endsWith("/") ? cleanBasePath : cleanBasePath + "/";
  }

  /**
   * Create a {@link HttpRequest.Builder} using the specified http method and resource. This client will
   * discover the service address and combine the specified resource in order to set a URL for the builder.
   *
   * @param method the request method
   * @param resource the request resource
   * @return a builder to create the http request, with method and URL already set
   */
  public HttpRequest.Builder requestBuilder(HttpMethod method, String resource) {
    return HttpRequest.builder(method, resolve(resource));
  }

  /**
   * Perform the request, returning the response. If there was a ConnectException while making the request,
   * a ServiceUnavailableException is thrown.
   *
   * @param request the request to perform
   * @return the response
   * @throws IOException if there was an IOException while performing the request
   * @throws ServiceUnavailableException if there was a ConnectException while making the request, or if the response
   *                                     was a 503
   */
  public HttpResponse execute(HttpRequest request) throws IOException {
    try {
      HttpResponse response = HttpRequests.execute(request, httpRequestConfig);
      if (response.getResponseCode() == HttpURLConnection.HTTP_UNAVAILABLE) {
        throw new ServiceUnavailableException(discoverableServiceName, response.getResponseBodyAsString());
      }
      return response;
    } catch (ConnectException e) {
      throw new ServiceUnavailableException(discoverableServiceName, e);
    }
  }

  /**
   * Discover the service address, then append the base path and specified resource to get the URL.
   *
   * @param resource the resource to use
   * @return the resolved URL
   * @throws ServiceUnavailableException if the service could not be discovered
   */
  public URL resolve(String resource) {
    Discoverable discoverable = endpointStrategySupplier.get().pick(1L, TimeUnit.SECONDS);
    if (discoverable == null) {
      throw new ServiceUnavailableException(discoverableServiceName);
    }
    InetSocketAddress address = discoverable.getSocketAddress();
    String scheme = Arrays.equals(Constants.Security.SSL_URI_SCHEME.getBytes(), discoverable.getPayload()) ?
      Constants.Security.SSL_URI_SCHEME : Constants.Security.URI_SCHEME;
    String urlStr = String.format("%s%s:%d%s%s", scheme, address.getHostName(), address.getPort(), basePath, resource);
    try {
      return new URL(urlStr);
    } catch (MalformedURLException e) {
      // shouldn't happen. If it does, it means there is some bug in the service announcer
      throw new IllegalStateException(String.format("Discovered service %s, but it announced malformed URL %s",
                                                    discoverableServiceName, urlStr), e);
    }
  }

  /**
   * Create a generic error message about a failure to make a specified request.
   *
   * @param request the request made
   * @param body the request body if it should be in the error message
   * @return a generic error message about the failure
   */
  public String createErrorMessage(HttpRequest request, @Nullable String body) {
    String headers = request.getHeaders() == null ?
      "null" : Joiner.on(",").withKeyValueSeparator("=").join(request.getHeaders().entries());
    return String.format("Error making request to %s service at %s while doing %s with headers %s%s.",
                         discoverableServiceName, request.getURL(), request.getMethod(),
                         headers, body == null ? "" : " and body " + body);
  }
}
