/*
 * Copyright © 2017-2022 Cask Data, Inc.
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
import io.cdap.cdap.api.retry.Idempotency;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.ServiceException;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.HttpCodes;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpContentConsumer;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import javax.net.ssl.HttpsURLConnection;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Discovers a remote service and resolves URLs to that service.
 */
public class RemoteClient {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteClient.class);

  public static final String RUNTIME_SERVICE_ROUTING_BASE_URI = "cdap.runtime.service.routing.base.uri";

  private final InternalAuthenticator internalAuthenticator;
  private final EndpointStrategy endpointStrategy;
  private final HttpRequestConfig httpRequestConfig;
  private final String discoverableServiceName;
  private final String basePath;
  private final RemoteAuthenticator remoteAuthenticator;

  RemoteClient(InternalAuthenticator internalAuthenticator, DiscoveryServiceClient discoveryClient,
      String discoverableServiceName, HttpRequestConfig httpRequestConfig, String basePath,
      RemoteAuthenticator remoteAuthenticator) {
    this.internalAuthenticator = internalAuthenticator;
    this.discoverableServiceName = discoverableServiceName;
    this.httpRequestConfig = httpRequestConfig;
    this.endpointStrategy = new RandomEndpointStrategy(
        () -> discoveryClient.discover(discoverableServiceName));
    String cleanBasePath = basePath.startsWith("/") ? basePath.substring(1) : basePath;
    this.basePath = cleanBasePath.endsWith("/") ? cleanBasePath : cleanBasePath + "/";
    this.remoteAuthenticator = remoteAuthenticator;
  }

  /**
   * Create a {@link HttpRequest.Builder} using the specified http method and resource. This client
   * will discover the service address and combine the specified resource in order to set a URL for
   * the builder.
   *
   * @param method the request method
   * @param resource the request resource
   * @return a builder to create the http request, with method and URL already set
   */
  public HttpRequest.Builder requestBuilder(HttpMethod method, String resource) {
    return HttpRequest.builder(method, resolve(resource));
  }

  private void setAuthHeader(BiConsumer<String, String> headerSetter, String header,
      String credentialType,
      String credentialValue) {
    headerSetter.accept(header, String.format("%s %s", credentialType, credentialValue));
  }

  /**
   * Perform the request, returning the response. If there was a ConnectException while making the
   * request, a ServiceUnavailableException is thrown. Wraps exceptions from {@link
   * RemoteClient#execute(HttpRequest)} into {@link RetryableException} that are retryable for
   * idempotent methods (GET/PUT/DELETE).
   *
   * @param request the request to perform
   * @return the response
   * @throws IOException if there was an IOException while performing the request
   * @throws ServiceUnavailableException if there was a ConnectException while making the
   *     request, or if the response was a 503
   * @throws RetryableException if there was an exception while performing an idempotent
   *     request
   */
  public HttpResponse execute(HttpRequest request) throws IOException, UnauthorizedException {
    return execute(request, Idempotency.AUTO);
  }

  /**
   * Perform the request, returning the response. Wraps exceptions from {@link
   * RemoteClient#execute(HttpRequest)} into {@link RetryableException} that are retryable for
   * idempotent operations.
   *
   * @param request the request to perform
   * @param idempotency the type of idempotency
   * @return the response
   * @throws IOException if there was an IOException while performing the non-idempotent
   *     request
   * @throws RetryableException if there was an exception while performing an idempotent
   *     request
   */
  public HttpResponse execute(HttpRequest request, Idempotency idempotency) throws IOException {
    switch (idempotency) {
      case IDEMPOTENT:
        return executeIdempotent(request);
      case AUTO:
        HttpMethod method = request.getMethod();
        if (method == HttpMethod.GET || method == HttpMethod.PUT || method == HttpMethod.DELETE) {
          return executeIdempotent(request);
        } // fall through
      default:
        return executeNonIdempotent(request);
    }
  }

  private HttpResponse executeIdempotent(HttpRequest request) {
    try {
      return executeNonIdempotent(request);
    } catch (IOException | ServiceException e) {
      throw new RetryableException(e);
    }
  }

  private HttpResponse executeNonIdempotent(HttpRequest request) throws IOException, UnauthorizedException {
    URL rewrittenUrl = rewriteUrl(request.getURL());
    Multimap<String, String> headers = setHeader(request);

    HttpRequest httpRequest = new HttpRequest(request.getMethod(), rewrittenUrl,
        headers, request.getBody(), request.getBodyLength());

    try {
      HttpResponse response = HttpRequests.execute(httpRequest, httpRequestConfig);
      int responseCode = response.getResponseCode();
      // 503 is always retryable. Other 5xx errors are retryable if the request is idempotent (handled in
      // RemoteClient#executeIdempotent(HttpRequest)
      if (responseCode == HttpURLConnection.HTTP_UNAVAILABLE) {
        throw new ServiceUnavailableException(discoverableServiceName,
            response.getResponseBodyAsString());
      }
      if (HttpCodes.isRetryable(responseCode)) {
        String contentType = response.getHeaders().get(HttpHeaders.CONTENT_TYPE).stream()
            .findFirst().orElse(null);
        String message;
        String jsonDetails = null;
        if ("application/json".equals(contentType)) {
          message = String.format("Service %s is not available (%d)", discoverableServiceName,
              responseCode);
          jsonDetails = response.getResponseBodyAsString();
        } else {
          message = String.format("Service %s is not available: %s", discoverableServiceName,
              response.getResponseBodyAsString());
        }
        throw new ServiceException(message, null,
            jsonDetails, HttpResponseStatus.valueOf(responseCode));
      }
      if (responseCode == HttpURLConnection.HTTP_FORBIDDEN) {
        throw new UnauthorizedException(response.getResponseBodyAsString());
      }
      return response;
    } catch (ConnectException e) {
      throw new ServiceUnavailableException(discoverableServiceName, e);
    }
  }

  /**
   * Makes a streaming {@link HttpRequest} and consumes the response using the {@link
   * HttpContentConsumer} provided in the request. It retries on failure.
   */
  public void executeStreamingRequest(HttpRequest request)
      throws IOException, UnauthorizedException {
    URL rewrittenUrl = rewriteUrl(request.getURL());
    Multimap<String, String> headers = setHeader(request);

    HttpRequest httpRequest = new HttpRequest(request.getMethod(), rewrittenUrl, headers,
        request.getBody(), request.getBodyLength(), request.getConsumer());
    HttpResponse httpResponse = HttpRequests.execute(httpRequest, httpRequestConfig);

    if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException(
          String.format("Request failed %s with code %d ", httpResponse.getResponseBodyAsString(),
              httpResponse.getResponseCode()));
    }
    httpResponse.consumeContent();
  }

  /**
   * Opens a {@link HttpURLConnection} for the given resource path.
   */
  public HttpURLConnection openConnection(String resource) throws IOException {
    URL url = resolve(resource);
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    if (urlConn instanceof HttpsURLConnection && !httpRequestConfig.isVerifySSLCert()) {
      new HttpsEnabler().setTrustAll(true).enable((HttpsURLConnection) urlConn);
    }
    if (urlConn instanceof HttpsURLConnection) {
      urlConn.connect();
      LOG.info("HTTPS Connection Suite: {}", ((HttpsURLConnection) urlConn).getCipherSuite());
    }
    urlConn.setConnectTimeout(httpRequestConfig.getConnectTimeout());
    urlConn.setReadTimeout(httpRequestConfig.getReadTimeout());
    urlConn.setDoInput(true);
    if (remoteAuthenticator != null) {
      Credential credential = remoteAuthenticator.getCredentials();
      if (credential != null) {
        setAuthHeader(urlConn::setRequestProperty, HttpHeaders.AUTHORIZATION,
            credential.getType().getQualifiedName(),
            credential.getValue());
      }
    }

    internalAuthenticator.applyInternalAuthenticationHeaders(urlConn::setRequestProperty);

    return urlConn;
  }

  /**
   * Opens a {@link HttpURLConnection} for the given request method on the given resource path.
   */
  public HttpURLConnection openConnection(HttpMethod method, String resource) throws IOException {
    HttpURLConnection urlConn = openConnection(resource);
    if (EnumSet.of(HttpMethod.POST, HttpMethod.PUT).contains(method)) {
      urlConn.setDoOutput(true);
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
      return rewriteUrl(uri.toURL());
    } catch (MalformedURLException e) {
      // shouldn't happen. If it does, it means there is some bug in the service announcer
      throw new IllegalStateException(
          String.format("Discovered service %s, but it announced malformed URL %s",
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
    String headers = request.getHeaders() == null ? "null" : Joiner.on(",")
        .withKeyValueSeparator("=")
        .join(request.getHeaders().entries());
    return String.format(
        "Error making request to %s service at %s while doing %s with headers %s%s.",
        discoverableServiceName, request.getURL(), request.getMethod(), headers,
        body == null ? "" : " and body " + body);
  }

  /**
   * Rewrites the given URL based on the runtime service.
   */
  private URL rewriteUrl(URL url) {
    if (url.getPort() != 0) {
      return url;
    }

    String baseUri = System.getProperty(RUNTIME_SERVICE_ROUTING_BASE_URI);
    if (baseUri == null) {
      return url;
    }
    try {
      String path = url.getFile();
      // Trim all the leading "/"
      while (!path.isEmpty() && path.charAt(0) == '/') {
        path = path.substring(1);
      }
      return URI.create(baseUri).resolve(discoverableServiceName + "/").resolve(path).toURL();
    } catch (IllegalArgumentException | MalformedURLException e) {
      return url;
    }
  }

  private Multimap<String, String> setHeader(HttpRequest request) throws IOException {
    Multimap<String, String> headers = request.getHeaders();
    headers = headers == null ? HashMultimap.create() : HashMultimap.create(headers);

    // Add Authorization header and use a rewritten URL if needed
    if (remoteAuthenticator != null && headers.keySet().stream()
        .noneMatch(HttpHeaders.AUTHORIZATION::equalsIgnoreCase)) {
      Credential credential = remoteAuthenticator.getCredentials();
      if (credential != null) {
        setAuthHeader(headers::put, HttpHeaders.AUTHORIZATION,
            credential.getType().getQualifiedName(),
            credential.getValue());
      }
    }

    internalAuthenticator.applyInternalAuthenticationHeaders(headers::put);
    return headers;
  }
}
