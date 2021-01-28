/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.capability;

import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.common.http.HttpContentConsumer;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Makes {@link io.cdap.common.http.HttpRequest}s, retrying on failure.
 */
public final class HttpClients {
  private static final int RETRY_LIMIT = 5;
  private static final int RETRY_DELAY = 5;

  private HttpClients() {
  }

  /**
   * Makes an {@link HttpRequest}, retries on failure.
   * @return {@link HttpResponse}
   */
  public static HttpResponse execute(HttpRequest request) throws IOException {
    return Retries.callWithRetries(() -> {
      HttpResponse httpResponse = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));

      if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
        throw new IOException(httpResponse.getResponseBodyAsString());
      }
      return httpResponse;
    }, RetryStrategies.limit(RETRY_LIMIT, RetryStrategies.fixDelay(RETRY_DELAY, TimeUnit.SECONDS)));
  }

  /**
   * Makes a streaming {@link HttpRequest} and consumes the response using the {@link HttpContentConsumer} provided
   * in the request. It retries on failure.
   */
  public static void executeStreamingRequest(HttpRequest request) throws IOException {
    if (!request.hasContentConsumer()) {
      throw new IllegalArgumentException("Request does not have content consumer");
    }

    Retries.callWithRetries(() -> {
      HttpResponse httpResponse = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));

      if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
        throw new IOException(httpResponse.getResponseBodyAsString());
      }
      httpResponse.consumeContent();
      return null;
    }, RetryStrategies.limit(RETRY_LIMIT, RetryStrategies.fixDelay(RETRY_DELAY, TimeUnit.SECONDS)));
  }

  /**
   * Does an HTTP GET on the given {@link URL}, retries on failure.
   * @return {@link String} response body
   */
  public static String doGetAsString(URL url) throws IOException {
    HttpRequest request = HttpRequest.get(url).build();
    return execute(request).getResponseBodyAsString();
  }

  /**
   * Does an HTTP GET on the given {@link URL}, retries on failure.
   * @return {@link byte[]} response body
   */
  public static byte[] doGetAsBytes(URL url) throws IOException {
    HttpRequest request = HttpRequest.get(url).build();
    return execute(request).getResponseBody();
  }
}
