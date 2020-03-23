/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.common.internal.remote;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.net.HttpHeaders;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.net.ssl.HttpsURLConnection;

/**
 * Discovers a remote service and resolves URLs to that service.
 */
public class RemoteClient {

  public static final String RUNTIME_SERVICE_ROUTING_BASE_URI = "cdap.runtime.service.routing.base.uri";

  private final EndpointStrategy endpointStrategy;
  private final HttpRequestConfig httpRequestConfig;
  private final String discoverableServiceName;
  private final String basePath;
  private final RemoteAuthenticator authenticator;

  public RemoteClient(DiscoveryServiceClient discoveryClient, String discoverableServiceName,
                      HttpRequestConfig httpRequestConfig, String basePath) {
    this(discoveryClient, discoverableServiceName, httpRequestConfig, basePath, null);
  }

  public RemoteClient(DiscoveryServiceClient discoveryClient, String discoverableServiceName,
                      HttpRequestConfig httpRequestConfig, String basePath,
                      @Nullable RemoteAuthenticator authenticator) {
    this.discoverableServiceName = discoverableServiceName;
    this.httpRequestConfig = httpRequestConfig;
    this.endpointStrategy = new RandomEndpointStrategy(() -> discoveryClient.discover(discoverableServiceName));
    String cleanBasePath = basePath.startsWith("/") ? basePath.substring(1) : basePath;
    this.basePath = cleanBasePath.endsWith("/") ? cleanBasePath : cleanBasePath + "/";
    this.authenticator = authenticator == null ? RemoteAuthenticator.getDefaultAuthenticator() : authenticator;
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
    HttpRequest httpRequest = request;
    URL rewrittenURL = rewriteURL(request.getURL());

    // Add Authorization header and use a rewritten URL if needed
    if (authenticator != null || !rewrittenURL.equals(request.getURL())) {
      Multimap<String, String> headers = request.getHeaders();
      if (authenticator != null) {
        headers = headers == null ? HashMultimap.create() : HashMultimap.create(headers);
        if (headers.keySet().stream().noneMatch(HttpHeaders.AUTHORIZATION::equalsIgnoreCase)) {
          headers.put(HttpHeaders.AUTHORIZATION,
                      String.format("%s %s", authenticator.getType(), authenticator.getCredentials()));
        }
      }
      httpRequest = new HttpRequest(request.getMethod(), rewrittenURL, headers,
                                    request.getBody(), request.getBodyLength());
    }

    try {
      HttpResponse response = HttpRequests.execute(httpRequest, httpRequestConfig);
      switch (response.getResponseCode()) {
        case HttpURLConnection.HTTP_UNAVAILABLE:
          throw new ServiceUnavailableException(discoverableServiceName, response.getResponseBodyAsString());
        case HttpURLConnection.HTTP_FORBIDDEN:
          throw new UnauthorizedException(response.getResponseBodyAsString());
        default:
          return response;
      }
    } catch (ConnectException e) {
      throw new ServiceUnavailableException(discoverableServiceName, e);
    }
  }

  /**
   * Opens a {@link HttpURLConnection} for the given request method on the given resource path.
   */
  public HttpURLConnection openConnection(HttpMethod method, String resource) throws IOException {
    URL url = resolve(resource);
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    if (urlConn instanceof HttpsURLConnection && !httpRequestConfig.isVerifySSLCert()) {
      new HttpsEnabler().setTrustAll(true).enable((HttpsURLConnection) urlConn);
    }
    urlConn.setConnectTimeout(httpRequestConfig.getConnectTimeout());
    urlConn.setReadTimeout(httpRequestConfig.getReadTimeout());
    urlConn.setDoInput(true);
    if (EnumSet.of(HttpMethod.POST, HttpMethod.PUT).contains(method)) {
      urlConn.setDoOutput(true);
    }
    if (authenticator != null) {
      urlConn.setRequestProperty(HttpHeaders.AUTHORIZATION,
                                 String.format("%s %s", authenticator.getType(), authenticator.getCredentials()));
    }

    urlConn.setRequestMethod(method.name());
    return urlConn;
  }

  /**
   * Discover the service address, then append the base path and specified resource to get the URL.
   *
   * @param resource the resource to use
   * @return the resolved URL
   * @throws ServiceUnavailableException if the service could not be discovered
   */
  public URL resolve(String resource) {
    Discoverable discoverable = endpointStrategy.pick(1L, TimeUnit.SECONDS);
    if (discoverable == null) {
      throw new ServiceUnavailableException(discoverableServiceName);
    }

    URI uri = URIScheme.createURI(discoverable, "%s%s", basePath, resource);
    try {
      return rewriteURL(uri.toURL());
    } catch (MalformedURLException e) {
      // shouldn't happen. If it does, it means there is some bug in the service announcer
      throw new IllegalStateException(String.format("Discovered service %s, but it announced malformed URL %s",
                                                    discoverableServiceName, uri), e);
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

  /**
   * Rewrites the given URL based on the runtime service.
   */
  private URL rewriteURL(URL url) {
    if (url.getPort() != 0) {
      return url;
    }

    String baseURI = System.getProperty(RUNTIME_SERVICE_ROUTING_BASE_URI);
    if (baseURI == null) {
      return url;
    }
    try {
      String path = url.getFile();
      // Trim all the leading "/"
      while (!path.isEmpty() && path.charAt(0) == '/') {
        path = path.substring(1);
      }
      return URI.create(baseURI).resolve(discoverableServiceName + "/").resolve(path).toURL();
    } catch (IllegalArgumentException | MalformedURLException e) {
      return url;
    }
  }
}
