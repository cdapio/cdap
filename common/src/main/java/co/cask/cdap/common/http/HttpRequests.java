/*
 * Copyright 2014 Cask Data, Inc.
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
package co.cask.cdap.common.http;

import com.google.common.collect.Multimap;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

/**
 * Executes {@link HttpRequest}s and returns an {@link HttpResponse}.
 */
public final class HttpRequests {

  private HttpRequests() { }

  /**
   * Executes an HTTP request to the url provided.
   *
   * @param request HTTP request to execute
   * @param requestConfig configuration for the HTTP request to execute
   * @return HTTP response
   */
  public static HttpResponse execute(HttpRequest request, HttpRequestConfig requestConfig) throws IOException {
    String requestMethod = request.getMethod().name();
    URL url = request.getURL();

    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod(requestMethod);
    conn.setReadTimeout(requestConfig.getReadTimeout());
    conn.setConnectTimeout(requestConfig.getConnectTimeout());

    Multimap<String, String> headers = request.getHeaders();
    if (headers != null) {
      for (Map.Entry<String, String> header : headers.entries()) {
        conn.setRequestProperty(header.getKey(), header.getValue());
      }
    }

    InputSupplier<? extends InputStream> bodySrc = request.getBody();
    if (bodySrc != null) {
      conn.setDoOutput(true);
    }

    conn.connect();

    try {
      if (bodySrc != null) {
        OutputStream os = conn.getOutputStream();
        try {
          ByteStreams.copy(bodySrc, os);
        } finally {
          os.close();
        }
      }

      try {
        if (isSuccessful(conn.getResponseCode())) {
          return new HttpResponse(conn.getResponseCode(), conn.getResponseMessage(),
                                  ByteStreams.toByteArray(conn.getInputStream()));
        }
      } catch (FileNotFoundException e) {
        // Server returns 404. Hence handle as error flow below. Intentional having empty catch block.
      }

      // Non 2xx response
      InputStream es = conn.getErrorStream();
      byte[] content = (es == null) ? new byte[0] : ByteStreams.toByteArray(es);
      return new HttpResponse(conn.getResponseCode(), conn.getResponseMessage(), content);
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Executes an HTTP request with default request configuration.
   *
   * @param request HTTP request to execute
   * @return HTTP response
   * @throws IOException
   */
  public static HttpResponse execute(HttpRequest request) throws IOException {
    return execute(request, HttpRequestConfig.DEFAULT);
  }

  private static boolean isSuccessful(int responseCode) {
    return 200 <= responseCode && responseCode < 300;
  }
}
