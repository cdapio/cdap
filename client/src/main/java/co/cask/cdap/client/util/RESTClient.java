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
import co.cask.cdap.client.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.common.http.HttpMethod;
import co.cask.cdap.common.http.HttpRequest;
import co.cask.cdap.common.http.HttpRequestConfig;
import co.cask.cdap.common.http.HttpRequests;
import co.cask.cdap.common.http.HttpResponse;
import co.cask.cdap.security.authentication.client.AccessToken;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.lang.ArrayUtils;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;

/**
 * Wrapper around the HTTP client implementation.
 */
public class RESTClient {

  private final HttpRequestConfig defaultConfig;
  private final HttpRequestConfig uploadConfig;

  private RESTClient(HttpRequestConfig defaultConfig, HttpRequestConfig uploadConfig) {
    this.defaultConfig = defaultConfig;
    this.uploadConfig = uploadConfig;
  }

  /**
   * Creates a default {@link RESTClient}.
   *
   * @param config {@link ClientConfig} that configures hostname and timeouts
   * @return {@link RESTClient} instance
   */
  public static RESTClient create(ClientConfig config) {
    return new RESTClient(config.getDefaultConfig(), config.getUploadConfig());
  }

  public HttpResponse execute(HttpRequest request, AccessToken accessToken, int... allowedErrorCodes)
    throws IOException, UnAuthorizedAccessTokenException {
    return execute(HttpRequest.builder(request).addHeaders(getAuthHeaders(accessToken)).build(), allowedErrorCodes);
  }

  public HttpResponse execute(HttpMethod httpMethod, URL url, AccessToken accessToken, int... allowedErrorCodes)
    throws IOException, UnAuthorizedAccessTokenException {
    return execute(HttpRequest.builder(httpMethod, url).addHeaders(getAuthHeaders(accessToken)).build(),
                   allowedErrorCodes);
  }

  public HttpResponse execute(HttpMethod httpMethod, URL url, Map<String, String> headers, AccessToken accessToken,
                              int... allowedErrorCodes) throws IOException, UnAuthorizedAccessTokenException {
    return execute(HttpRequest.builder(httpMethod, url).addHeaders(headers).addHeaders(getAuthHeaders(accessToken))
                     .build(), allowedErrorCodes);
  }

  private HttpResponse execute(HttpRequest request, int... allowedErrorCodes) throws IOException,
    UnAuthorizedAccessTokenException {
    HttpResponse response = HttpRequests.execute(request, defaultConfig);
    int responseCode = response.getResponseCode();
    if (responseCode == HttpStatus.SC_UNAUTHORIZED) {
      throw new UnAuthorizedAccessTokenException("Unauthorized status code received from the server.");
    } else if (!isSuccessful(responseCode) && !ArrayUtils.contains(allowedErrorCodes, responseCode)) {
      throw new IOException(response.getResponseBodyAsString());
    }
    return response;
  }

  public HttpResponse upload(HttpRequest request, AccessToken accessToken, int... allowedErrorCodes)
    throws IOException {
    HttpResponse response = HttpRequests.execute(
      HttpRequest.builder(request).addHeaders(getAuthHeaders(accessToken)).build(), uploadConfig);
    int responseCode = response.getResponseCode();
    if (!isSuccessful(responseCode) && !ArrayUtils.contains(allowedErrorCodes, responseCode)) {
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
}
