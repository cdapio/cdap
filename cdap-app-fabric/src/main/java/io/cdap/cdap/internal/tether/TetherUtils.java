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

package io.cdap.cdap.internal.tether;

import com.google.common.net.HttpHeaders;
import io.cdap.cdap.common.internal.remote.RemoteAuthenticator;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;

import java.io.IOException;
import java.net.URI;
import javax.annotation.Nullable;

/**
 * Utility functions for tethering.
 */
public class TetherUtils {
  // Connection timeout = 5 seconds.
  private static final int TIMEOUT_MS = 5000;
  private static final RemoteAuthenticator AUTHENTICATOR = RemoteAuthenticator.getDefaultAuthenticator();

  private TetherUtils() {
  }

  public static HttpResponse sendHttpRequest(HttpMethod method, URI endpoint) throws IOException {
    return sendHttpRequest(method, endpoint, null);
  }

  public static HttpResponse sendHttpRequest(HttpMethod method, URI endpoint, @Nullable String content)
    throws IOException {
    io.cdap.common.http.HttpRequest.Builder builder;
    switch (method) {
      case GET:
        builder = io.cdap.common.http.HttpRequest.get(endpoint.toURL());
        break;
      case PUT:
        builder = io.cdap.common.http.HttpRequest.put(endpoint.toURL());
        break;
      case POST:
        builder = io.cdap.common.http.HttpRequest.post(endpoint.toURL());
        break;
      case DELETE:
        builder = io.cdap.common.http.HttpRequest.delete(endpoint.toURL());
        break;
      default:
        throw new RuntimeException("Unexpected HTTP method: " + method);
    }
    if (content != null) {
      builder.withBody(content);
    }
    // Add Authorization header.
    if (AUTHENTICATOR != null) {
      builder.addHeader(HttpHeaders.AUTHORIZATION,
                        String.format("%s %s", AUTHENTICATOR.getType(), AUTHENTICATOR.getCredentials()));
    }
    return HttpRequests.execute(builder.build(), new HttpRequestConfig(TIMEOUT_MS, TIMEOUT_MS));
  }
}
