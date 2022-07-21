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

package io.cdap.cdap.client.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.exception.DisconnectedException;
import io.cdap.cdap.security.authentication.client.AccessToken;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.commons.lang.ArrayUtils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.HttpHeaders;

/**
 * Wrapper around the HTTP client implementation.
 */
public class RESTClient {

  private final List<Listener> listeners;
  private final ClientConfig clientConfig;

  @Inject
  public RESTClient(ClientConfig clientConfig) {
    this.clientConfig = clientConfig;
    this.listeners = Lists.newArrayList();
  }

  public RESTClient() {
    this(ClientConfig.getDefault());
  }

  public void addListener(Listener listener) {
    listeners.add(listener);
  }

  public boolean removeListener(Listener listener) {
    return listeners.remove(listener);
  }

  public HttpResponse execute(HttpRequest request, AccessToken accessToken, int... allowedErrorCodes)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    return execute(HttpRequest.builder(request).addHeaders(getAuthHeaders(accessToken)).build(), allowedErrorCodes);
  }

  public HttpResponse execute(HttpMethod httpMethod, URL url, AccessToken accessToken, int... allowedErrorCodes)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    return execute(HttpRequest.builder(httpMethod, url)
                     .addHeaders(getAuthHeaders(accessToken))
                     .build(), allowedErrorCodes);
  }

  public HttpResponse execute(HttpMethod httpMethod, URL url, Map<String, String> headers, AccessToken accessToken,
                              int... allowedErrorCodes)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    return execute(HttpRequest.builder(httpMethod, url)
                     .addHeaders(headers)
                     .addHeaders(getAuthHeaders(accessToken))
                     .build(), allowedErrorCodes);
  }

  public HttpResponse execute(HttpMethod httpMethod, URL url, String body, Map<String, String> headers,
                              AccessToken accessToken, int... allowedErrorCodes)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    return execute(HttpRequest.builder(httpMethod, url)
                     .addHeaders(headers)
                     .addHeaders(getAuthHeaders(accessToken))
                     .withBody(body).build(), allowedErrorCodes);
  }

  public HttpResponse execute(HttpRequest request, int... allowedErrorCodes)
    throws IOException, UnauthenticatedException, UnauthorizedException {

    int currentTry = 0;
    HttpResponse response;
    int responseCode;
    boolean allowUnavailable = ArrayUtils.contains(allowedErrorCodes, HttpURLConnection.HTTP_UNAVAILABLE);

    do {
      onRequest(request, currentTry);
      response = HttpRequests.execute(request, clientConfig.getDefaultRequestConfig());
      responseCode = response.getResponseCode();

      if (responseCode != HttpURLConnection.HTTP_UNAVAILABLE || allowUnavailable) {
        // only retry if unavailable
        break;
      }

      currentTry++;
      try {
        TimeUnit.MILLISECONDS.sleep(1000);
      } catch (InterruptedException e) {
        break;
      }
    } while (currentTry <= clientConfig.getUnavailableRetryLimit());

    onResponse(request, response, currentTry);

    if (responseCode == HttpURLConnection.HTTP_UNAUTHORIZED) {
      throw new UnauthenticatedException("Unauthorized status code received from the server.");
    }
    if (responseCode == HttpURLConnection.HTTP_FORBIDDEN) {
      throw new UnauthorizedException(response.getResponseBodyAsString());
    }
    if (!isSuccessful(responseCode) && !ArrayUtils.contains(allowedErrorCodes, responseCode)) {
      throw new IOException(responseCode + ": " + response.getResponseBodyAsString());
    }
    return response;
  }

  private void onRequest(HttpRequest request, int attempt) {
    for (Listener listener : listeners) {
      listener.onRequest(request, attempt);
    }
  }

  private void onResponse(HttpRequest request, HttpResponse response, int attemptsMade) {
    for (Listener listener : listeners) {
      listener.onResponse(request, response, attemptsMade);
    }
  }

  public HttpResponse upload(HttpRequest request, AccessToken accessToken, int... allowedErrorCodes)
    throws IOException, UnauthenticatedException, DisconnectedException {

    HttpResponse response = HttpRequests.execute(
      HttpRequest.builder(request).addHeaders(getAuthHeaders(accessToken)).build(),
      clientConfig.getUploadRequestConfig());
    int responseCode = response.getResponseCode();
    if (!isSuccessful(responseCode) && !ArrayUtils.contains(allowedErrorCodes, responseCode)) {
      if (responseCode == HttpURLConnection.HTTP_UNAUTHORIZED) {
        throw new UnauthenticatedException("Unauthorized status code received from the server.");
      }
      throw new IOException(response.getResponseBodyAsString());
    }
    return response;
  }

  private boolean isSuccessful(int responseCode) {
    return 200 <= responseCode && responseCode <= 299;
  }

  private Map<String, String> getAuthHeaders(AccessToken accessToken) {
    Map<String, String> headers = clientConfig.getAdditionalHeaders();
    if (accessToken != null) {
      ImmutableMap.Builder<String, String> headersBuilder = ImmutableMap.builder();
      headersBuilder.putAll(headers);
      headersBuilder.put(HttpHeaders.AUTHORIZATION, accessToken.getTokenType() + " " + accessToken.getValue());
      headers = headersBuilder.build();
    }
    return headers;
  }

  /**
   * Listener for when requests are made and when responses are received.
   */
  public interface Listener {
    void onRequest(HttpRequest request, int attempt);
    void onResponse(HttpRequest request, HttpResponse response, int attemptsMade);
  }
}
