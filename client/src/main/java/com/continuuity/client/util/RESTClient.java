/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.client.util;

import com.continuuity.client.config.ReactorClientConfig;
import com.continuuity.common.http.HttpMethod;
import com.continuuity.common.http.HttpRequest;
import com.continuuity.common.http.HttpRequestConfig;
import com.continuuity.common.http.HttpRequests;
import com.continuuity.common.http.HttpResponse;
import org.apache.commons.lang.ArrayUtils;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

/**
 * Wrapper around the HTTP client implementation.
 */
public class RESTClient {

  private final HttpRequestConfig defaultConfig;
  private final HttpRequestConfig uploadConfig;

  public RESTClient(HttpRequestConfig defaultConfig, HttpRequestConfig uploadConfig) {
    this.defaultConfig = defaultConfig;
    this.uploadConfig = uploadConfig;
  }

  /**
   * Creates a default {@link RESTClient}.
   *
   * @param config {@link ReactorClientConfig} that provides information about the Reactor host and timeouts
   * @return {@link RESTClient} instance
   */
  public static RESTClient create(ReactorClientConfig config) {
    return new RESTClient(config.getDefaultConfig(), config.getUploadConfig());
  }

  public HttpResponse execute(HttpRequest request, int... allowedErrorCodes) throws IOException {
    HttpResponse response = HttpRequests.execute(request, defaultConfig);
    if (!isSuccessful(response.getResponseCode())
      && !ArrayUtils.contains(allowedErrorCodes, response.getResponseCode())) {
      throw new IOException(response.getResponseBodyAsString());
    }
    return response;
  }

  public HttpResponse execute(HttpMethod httpMethod, URL url, int... allowedErrorCodes) throws IOException {
    return execute(HttpRequest.builder(httpMethod, url).build(), allowedErrorCodes);
  }

  public HttpResponse execute(HttpMethod httpMethod, URL url, Map<String, String> headers,
                              int... allowedErrorCodes) throws IOException {
    return execute(HttpRequest.builder(httpMethod, url).addHeaders(headers).build(), allowedErrorCodes);
  }

  public HttpResponse upload(HttpRequest request, int... allowedErrorCodes) throws IOException {
    HttpResponse response = HttpRequests.execute(request, uploadConfig);
    if (!isSuccessful(response.getResponseCode())
      && !ArrayUtils.contains(allowedErrorCodes, response.getResponseCode())) {
      throw new IOException(response.getResponseBodyAsString());
    }
    return response;
  }

  private boolean isSuccessful(int responseCode) {
    return 200 <= responseCode && responseCode <= 299;
  }

}
