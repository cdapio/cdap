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

package co.cask.cdap.client.util;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.exception.DisconnectedException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.security.authentication.client.AccessToken;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
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
    throws IOException, UnauthorizedException, DisconnectedException {
    return execute(HttpRequest.builder(request).addHeaders(getAuthHeaders(accessToken)).build(), allowedErrorCodes);
  }

  public HttpResponse execute(HttpMethod httpMethod, URL url, AccessToken accessToken, int... allowedErrorCodes)
    throws IOException, UnauthorizedException, DisconnectedException {
    return execute(HttpRequest.builder(httpMethod, url)
                     .addHeaders(getAuthHeaders(accessToken))
                     .build(), allowedErrorCodes);
  }

  public HttpResponse execute(HttpMethod httpMethod, URL url, Map<String, String> headers, AccessToken accessToken,
                              int... allowedErrorCodes)
    throws IOException, UnauthorizedException, DisconnectedException {
    return execute(HttpRequest.builder(httpMethod, url)
                     .addHeaders(headers)
                     .addHeaders(getAuthHeaders(accessToken))
                     .build(), allowedErrorCodes);
  }

  public HttpResponse execute(HttpMethod httpMethod, URL url, String body, Map<String, String> headers,
                              AccessToken accessToken, int... allowedErrorCodes)
    throws IOException, UnauthorizedException, DisconnectedException {
    return execute(HttpRequest.builder(httpMethod, url)
                     .addHeaders(headers)
                     .addHeaders(getAuthHeaders(accessToken))
                     .withBody(body).build(), allowedErrorCodes);
  }

  private HttpResponse execute(HttpRequest request, int... allowedErrorCodes)
    throws IOException, UnauthorizedException, DisconnectedException {

    int currentTry = 0;
    HttpResponse response;
    do {
      onRequest(request, currentTry);
      response = HttpRequests.execute(request, clientConfig.getDefaultRequestConfig());

      int responseCode = response.getResponseCode();
      if (responseCode == HttpURLConnection.HTTP_UNAVAILABLE) {
        currentTry++;
        try {
          TimeUnit.MILLISECONDS.sleep(100);
        } catch (InterruptedException e) {
          break;
        }
        continue;
      }

      onResponse(request, response, currentTry);
      if (responseCode == HttpURLConnection.HTTP_UNAUTHORIZED) {
        throw new UnauthorizedException("Unauthorized status code received from the server.");
      }
      if (!isSuccessful(responseCode) && !ArrayUtils.contains(allowedErrorCodes, responseCode)) {
        throw new IOException(responseCode + ": " + response.getResponseBodyAsString());
      }
      return response;
    } while (currentTry <= clientConfig.getUnavailableRetryLimit());
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
    throws IOException, UnauthorizedException, DisconnectedException {

    HttpResponse response = HttpRequests.execute(
      HttpRequest.builder(request).addHeaders(getAuthHeaders(accessToken)).build(),
      clientConfig.getUploadRequestConfig());
    int responseCode = response.getResponseCode();
    if (!isSuccessful(responseCode) && !ArrayUtils.contains(allowedErrorCodes, responseCode)) {
      if (responseCode == HttpURLConnection.HTTP_UNAUTHORIZED) {
        throw new UnauthorizedException("Unauthorized status code received from the server.");
      }
      throw new IOException(response.getResponseBodyAsString());
    }
    return response;
  }

  private boolean isSuccessful(int responseCode) {
    return 200 <= responseCode && responseCode <= 299;
  }

  private Map<String, String> getAuthHeaders(AccessToken accessToken) {
    Map<String, String> headers = ImmutableMap.of();
    if (accessToken != null) {
      headers = ImmutableMap.of(HttpHeaders.AUTHORIZATION, accessToken.getTokenType() + " " + accessToken.getValue());
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
