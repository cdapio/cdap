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

package io.cdap.cdap.internal.tethering;

import com.google.common.net.HttpHeaders;
import io.cdap.cdap.common.internal.remote.RemoteAuthenticator;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;

import java.io.IOException;
import java.net.URI;
import javax.annotation.Nullable;

/**
 * Utility functions for tethering.
 */
public final class TetheringUtils {
  // Connection timeout = 5 seconds.
  private static final int TIMEOUT_MS = 5000;

  private TetheringUtils() {
  }

  public static HttpResponse sendHttpRequest(HttpMethod method, URI endpoint) throws IOException {
    return sendHttpRequest(method, endpoint, null);
  }

  public static HttpResponse sendHttpRequest(HttpMethod method, URI endpoint, @Nullable String content)
    throws IOException {
   HttpRequest.Builder builder;
    switch (method) {
      case GET:
        builder = HttpRequest.get(endpoint.toURL());
        break;
      case PUT:
        builder = HttpRequest.put(endpoint.toURL());
        break;
      case POST:
        builder = HttpRequest.post(endpoint.toURL());
        break;
      case DELETE:
        builder = HttpRequest.delete(endpoint.toURL());
        break;
      default:
        throw new RuntimeException("Unexpected HTTP method: " + method);
    }
    if (content != null && !content.isEmpty()) {
      builder.withBody(content);
    }

    // Add Authorization header.
    RemoteAuthenticator authenticator = RemoteAuthenticator.getDefaultAuthenticator();
    if (authenticator != null) {
      builder.addHeader(HttpHeaders.AUTHORIZATION,
                        String.format("%s %s", authenticator.getType(), authenticator.getCredentials()));
    }
    return HttpRequests.execute(builder.build(), new HttpRequestConfig(TIMEOUT_MS, TIMEOUT_MS));
  }
}
